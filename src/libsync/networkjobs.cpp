/*
 * Copyright (C) by Klaas Freitag <freitag@owncloud.com>
 * Copyright (C) by Daniel Molkentin <danimo@owncloud.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 */

#include <QBuffer>
#include <QCoreApplication>
#include <QJsonDocument>
#include <QLoggingCategory>
#include <QMutex>
#include <QNetworkAccessManager>
#include <QNetworkReply>
#include <QNetworkRequest>
#include <QSslCipher>
#include <QSslConfiguration>
#include <QStack>
#include <QStringList>
#include <QTimer>
#include <QXmlStreamReader>
#include <QPainter>
#include <QPainterPath>
#include <QObject>
#include <QRandomGenerator>
#include <QFile>

#include "creds/httpcredentials.h"

#include "account.h"
#include "networkjobs.h"
#include "qstring.h"
#include "filesystem.h"

#include "minio-cpp/include/client.h"
#include "syncfileitem.h"
#include <QStringList>
#include <QString>
#include <QList>

using namespace std::chrono_literals;

namespace OCC {

Q_LOGGING_CATEGORY(lcEtagJob, "sync.networkjob.etag", QtInfoMsg)
Q_LOGGING_CATEGORY(lcPropfindJob, "sync.networkjob.propfind", QtInfoMsg)
Q_LOGGING_CATEGORY(lcAvatarJob, "sync.networkjob.avatar", QtInfoMsg)
Q_LOGGING_CATEGORY(lcMkColJob, "sync.networkjob.mkcol", QtInfoMsg)
Q_LOGGING_CATEGORY(lcDetermineAuthTypeJob, "sync.networkjob.determineauthtype", QtInfoMsg)

Q_LOGGING_CATEGORY(lcMinioJob, "sync.networkjob.miniojob", QtInfoMsg)

RequestEtagJob::RequestEtagJob(AccountPtr account, const QUrl &rootUrl, const QString &path, QObject *parent) : AbstractNetworkJob(account, rootUrl, path, parent)
{
    QObject::connect(this, &RequestEtagJob::directoryListingIteratedS3, this, [=](const minio::s3::Item &item, const QMap<QString, QString> &properties) {
        _etag = Utility::normalizeEtag(QString::fromStdString(item.etag));
    });
}

void RequestEtagJob::start() {
    /*
     * if path == "/" -> list buckets
     * else list objects based on path
     */
    qCDebug(lcEtagJob) << "will try to get etags from path" << path();

    QList<QString> bucketList;
    QStringList folders;

    auto creds = _account->credentials();

    qCDebug(lcEtagJob) << "user" << creds->user();

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    if (path() == QStringLiteral("/") || path() == QStringLiteral("")) {
        qCWarning(lcEtagJob) << "probably we should not be here";
        qCWarning(lcEtagJob) << "we are trying to get etag from path " << path();

        return;
    }

    std::string bucket;
    std::string prefix;
    const QStringList __path = path().split(QStringLiteral("/"));
    if (__path.size() >= 1) {
        bucket = __path[1].toStdString();
    }

    qCDebug(lcEtagJob) << "first element" << __path[1];
    qCDebug(lcEtagJob) << "path size" << __path.size();

    if (__path.size() > 2) {
        prefix = std::regex_replace(prefix, std::regex(bucket), "");
    }

    qCDebug(lcEtagJob) << "prefix" << QString::fromStdString(prefix);

    QUrl baseUrl;
    QString subPath;
    auto job = new MinioJob(_account, baseUrl, subPath, MinioJob::Depth::One, this);
    QMap<QString, QString> currentHttp200Properties;
    minio::s3::Item item;

    if (prefix.empty()) {
        auto rlmDatetime = job->getBucketTag(bucket, "rlm_datetime");

        if (rlmDatetime == "") {
            qCDebug(lcEtagJob) << "etag not set for bucket" << QString::fromStdString(bucket) << "so we gonna set it to current datetime";
            rlmDatetime = job->getOrUpdateAndGetRLMDatetime(bucket, prefix);
            _etag = QString::fromStdString(rlmDatetime);
        }

        item.etag = rlmDatetime;

        emit RequestEtagJob::directoryListingIteratedS3(item, currentHttp200Properties);
        emit RequestEtagJob::finishedSignal({});

        return;
    }

    item.etag = job->getOrUpdateAndGetRLMDatetime(bucket, prefix);

    emit RequestEtagJob::directoryListingIteratedS3(item, currentHttp200Properties);
    emit RequestEtagJob::finishedSignal({});
}

const QString &RequestEtagJob::etag() const
{
    return _etag;
}

void RequestEtagJob::finished()
{
    qCInfo(lcEtagJob) << "RequestEtagJob finished";
}

/*********************************************************************************************/

MkColJob::MkColJob(AccountPtr account, const QUrl &url, const QString &path,
    const QMap<QByteArray, QByteArray> &extraHeaders, QObject *parent)
    : AbstractNetworkJob(account, url, path, parent)
    , _extraHeaders(extraHeaders)
{
}

void MkColJob::start()
{
    // add 'Content-Length: 0' header (see https://github.com/owncloud/client/issues/3256)
    QNetworkRequest req;
    req.setRawHeader("Content-Length", "0");
    for (auto it = _extraHeaders.constBegin(); it != _extraHeaders.constEnd(); ++it) {
        req.setRawHeader(it.key(), it.value());
    }

    // assumes ownership
    sendRequest("MKCOL", req);
    AbstractNetworkJob::start();
}

void MkColJob::finished()
{
    qCInfo(lcMkColJob) << "MKCOL of" << reply()->request().url() << "FINISHED WITH STATUS"
                       << replyStatusString();

    if (reply()->error() != QNetworkReply::NoError) {
        Q_EMIT finishedWithError(reply());
    } else {
        Q_EMIT finishedWithoutError();
    }
}

/*********************************************************************************************/
// supposed to read <D:collection> when pointing to <D:resourcetype><D:collection></D:resourcetype>..
static QString readContentsAsString(QXmlStreamReader &reader)
{
    QString result;
    int level = 0;
    do {
        QXmlStreamReader::TokenType type = reader.readNext();
        if (type == QXmlStreamReader::StartElement) {
            level++;
            result += QLatin1Char('<') + reader.name().toString() + QLatin1Char('>');
        } else if (type == QXmlStreamReader::Characters) {
            result += reader.text();
        } else if (type == QXmlStreamReader::EndElement) {
            level--;
            if (level < 0) {
                break;
            }
            result += QStringLiteral("</") + reader.name().toString() + QLatin1Char('>');
        }

    } while (!reader.atEnd());
    return result;
}


LsColXMLParser::LsColXMLParser()
{
}

bool LsColXMLParser::parse(const QByteArray &xml, QHash<QString, qint64> *sizes, const QString &expectedPath)
{
    // Parse DAV response
    QXmlStreamReader reader(xml);
    reader.addExtraNamespaceDeclaration(QXmlStreamNamespaceDeclaration(QStringLiteral("d"), QStringLiteral("DAV:")));

    QStringList folders;
    QString currentHref;
    QMap<QString, QString> currentTmpProperties;
    QMap<QString, QString> currentHttp200Properties;
    bool currentPropsAreValid = false;
    bool insidePropstat = false;
    bool insideProp = false;
    bool insideMultiStatus = false;

    while (!reader.atEnd()) {
        QXmlStreamReader::TokenType type = reader.readNext();
        QString name = reader.name().toString();
        // Start elements with DAV:
        if (type == QXmlStreamReader::StartElement && reader.namespaceUri() == QLatin1String("DAV:")) {
            if (name == QLatin1String("href")) {
                // We don't use URL encoding in our request URL (which is the expected path) (QNAM will do it for us)
                // but the result will have URL encoding..
                QString hrefString = QString::fromUtf8(QByteArray::fromPercentEncoding(reader.readElementText().toUtf8()));
                if (!hrefString.startsWith(expectedPath)) {
                    qCWarning(lcPropfindJob) << "Invalid href" << hrefString << "expected starting with" << expectedPath;
                    return false;
                }
                currentHref = hrefString;
            } else if (name == QLatin1String("response")) {
            } else if (name == QLatin1String("propstat")) {
                insidePropstat = true;
            } else if (name == QLatin1String("status") && insidePropstat) {
                QString httpStatus = reader.readElementText();
                if (httpStatus.startsWith(QLatin1String("HTTP/1.1 200")) || httpStatus.startsWith(QLatin1String("HTTP/1.1 425"))) {
                    currentPropsAreValid = true;
                } else {
                    currentPropsAreValid = false;
                }
            } else if (name == QLatin1String("prop")) {
                insideProp = true;
                continue;
            } else if (name == QLatin1String("multistatus")) {
                insideMultiStatus = true;
                continue;
            }
        }

        if (type == QXmlStreamReader::StartElement && insidePropstat && insideProp) {
            // All those elements are properties
            QString propertyContent = readContentsAsString(reader);
            if (name == QLatin1String("resourcetype") && propertyContent.contains(QLatin1String("collection"))) {
                folders.append(currentHref);
            } else if (name == QLatin1String("size")) {
                bool ok = false;
                auto s = propertyContent.toLongLong(&ok);
                if (ok && sizes) {
                    sizes->insert(currentHref, s);
                }
            }
            currentTmpProperties.insert(reader.name().toString(), propertyContent);
        }

        // End elements with DAV:
        if (type == QXmlStreamReader::EndElement) {
            if (reader.namespaceUri() == QLatin1String("DAV:")) {
                if (reader.name() == QLatin1String("response")) {
                    if (currentHref.endsWith(QLatin1Char('/'))) {
                        currentHref.chop(1);
                    }
                    emit directoryListingIterated(currentHref, currentHttp200Properties);
                    currentHref.clear();
                    currentHttp200Properties.clear();
                } else if (reader.name() == QLatin1String("propstat")) {
                    insidePropstat = false;
                    if (currentPropsAreValid) {
                        currentHttp200Properties = std::move(currentTmpProperties);
                    }
                    currentPropsAreValid = false;
                } else if (reader.name() == QLatin1String("prop")) {
                    insideProp = false;
                }
            }
        }
    }

    if (reader.hasError()) {
        // XML Parser error? Whatever had been emitted before will come as directoryListingIterated
        qCWarning(lcPropfindJob) << "ERROR" << reader.errorString() << xml;
        return false;
    } else if (!insideMultiStatus) {
        qCWarning(lcPropfindJob) << "ERROR no WebDAV response?" << xml;
        return false;
    } else {
        emit directoryListingSubfolders(folders);
        emit finishedWithoutError();
    }
    return true;
}

/*********************************************************************************************/

PropfindJob::PropfindJob(AccountPtr account, const QUrl &url, const QString &path, Depth depth, QObject *parent)
    : AbstractNetworkJob(account, url, path, parent)
    , _depth(depth)
{
    // Always have a higher priority than the propagator because we use this from the UI
    // and really want this to be done first (no matter what internal scheduling QNAM uses).
    // Also possibly useful for avoiding false timeouts.
    setPriority(QNetworkRequest::HighPriority);
}

MinioJob::MinioJob(AccountPtr account, const QUrl &url, const QString &path, Depth depth, QObject *parent)
    : AbstractNetworkJob(account, url, path, parent)
    , _depth(depth)
{
}

void PropfindJob::setProperties(const QList<QByteArray> &properties)
{
    _properties = properties;
}

QList<QByteArray> PropfindJob::properties() const
{
    return _properties;
}

void PropfindJob::start()
{
    QNetworkRequest req;
    req.setRawHeader(QByteArrayLiteral("Depth"), QByteArray::number(static_cast<int>(_depth)));
    req.setRawHeader(QByteArrayLiteral("Prefer"), QByteArrayLiteral("return=minimal"));

    if (_properties.isEmpty()) {
        qCWarning(lcPropfindJob) << "Propfind with no properties!";
    }
    QByteArray data;
    {
        QTextStream stream(&data, QIODevice::WriteOnly);
        stream.setEncoding(QStringConverter::Utf8);
        stream << QByteArrayLiteral("<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                                    "<d:propfind xmlns:d=\"DAV:\">"
                                    "<d:prop>");

        for (const QByteArray &prop : qAsConst(_properties)) {
            const int colIdx = prop.lastIndexOf(':');
            if (colIdx >= 0) {
                stream << QByteArrayLiteral("<") << prop.mid(colIdx + 1) << QByteArrayLiteral(" xmlns=\"") << prop.left(colIdx) << QByteArrayLiteral("\"/>");
            } else {
                stream << QByteArrayLiteral("<d:") << prop << QByteArrayLiteral("/>");
            }
        }
        stream << QByteArrayLiteral("</d:prop>"
                                    "</d:propfind>\n");
    }

    QBuffer *buf = new QBuffer(this);
    buf->setData(data);
    buf->open(QIODevice::ReadOnly);
    sendRequest(QByteArrayLiteral("PROPFIND"), req, buf);
    AbstractNetworkJob::start();
}

std::string MinioJob::getObjectTag(const std::string &bucket, const std::string &name, const std::string &key)
{
    auto creds = _account->credentials();

    qCDebug(lcMinioJob) << "getting object tag";
    qCDebug(lcMinioJob) << "user" << creds->user();
    qCDebug(lcMinioJob) << "bucket" << QString::fromStdString(bucket);
    qCDebug(lcMinioJob) << "object" << QString::fromStdString(name);
    qCDebug(lcMinioJob) << "key" << QString::fromStdString(key);

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    minio::s3::GetObjectTagsArgs objectTagsArgs;
    objectTagsArgs.bucket = bucket;
    objectTagsArgs.object = name;

    minio::s3::GetObjectTagsResponse objectTagResponse = client.GetObjectTags(objectTagsArgs);
    if (!objectTagResponse) {
        qCDebug(lcMinioJob) << "unable to get tag for ↑↑↑";
        qCDebug(lcMinioJob) << QString::fromStdString(objectTagResponse.Error().String());
        return "";
    }

    for (auto& [tagKey, tagValue] : objectTagResponse.tags) {
        qCDebug(lcMinioJob) << "Tag: Key:" << QString::fromStdString(tagKey) << ", " << "Value:" << QString::fromStdString(tagValue);

        if (key == tagKey) {
            return tagValue;
        }
    }

    return "";
}

std::string MinioJob::getBucketTag(const std::string &bucket, const std::string &key)
{
    auto creds = _account->credentials();

    qCDebug(lcMinioJob) << "getting bucket tag";
    qCDebug(lcMinioJob) << "user" << creds->user();
    qCDebug(lcMinioJob) << "bucket" << QString::fromStdString(bucket);
    qCDebug(lcMinioJob) << "key" << QString::fromStdString(key);

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    minio::s3::GetBucketTagsArgs bucketTagsArgs;
    bucketTagsArgs.bucket = bucket;

    minio::s3::GetBucketTagsResponse bucketTagResponse = client.GetBucketTags(bucketTagsArgs);
    if (!bucketTagResponse) {
        qCDebug(lcMinioJob) << "unable to get bucket tag for ↑↑↑";
        qCDebug(lcMinioJob) << QString::fromStdString(bucketTagResponse.Error().String());
        return "";
    }

    for (auto& [tagKey, tagValue] : bucketTagResponse.tags) {
        qCDebug(lcMinioJob) << "Tag: Key:" << QString::fromStdString(tagKey) << ", " << "Value:" << QString::fromStdString(tagValue);

        if (key == tagKey) {
            return tagValue;
        }
    }

    return "";
}

minio::s3::GetObjectTagsResponse MinioJob::setObjectTag(const std::string &bucket, const std::string &name, const std::string &key, const std::string &value, const bool createIfNoKey)
{
    auto creds = _account->credentials();

    qCDebug(lcMinioJob) << "setting object tag";
    qCDebug(lcMinioJob) << "user" << creds->user();
    qCDebug(lcMinioJob) << "bucket" << QString::fromStdString(bucket);
    qCDebug(lcMinioJob) << "object" << QString::fromStdString(name);
    qCDebug(lcMinioJob) << "key" << QString::fromStdString(key);
    qCDebug(lcMinioJob) << "value" << QString::fromStdString(value);

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    if (createIfNoKey) {
        minio::s3::StatObjectArgs statObjectArgs;
        statObjectArgs.bucket = bucket;
        statObjectArgs.object = name;

        minio::s3::StatObjectResponse statObjectResponse = client.StatObject(statObjectArgs);

        if (!statObjectResponse) {
            qCDebug(lcMinioJob) << "unable to get stat object" << QString::fromStdString(statObjectResponse.Error().String());
            qCDebug(lcMinioJob) << "no object exists, we'll create one";

            std::ifstream file;

            minio::s3::PutObjectArgs putObjectArgs(file, 0, 0);
            putObjectArgs.bucket = bucket;
            putObjectArgs.object = name;

            minio::s3::PutObjectResponse putObjectResponse = client.PutObject(putObjectArgs);
            if (!putObjectResponse) {
                qCDebug(lcMinioJob) << "unable create object or folder ↑↑↑";
                qCDebug(lcMinioJob) << QString::fromStdString(putObjectResponse.Error().String());
            }
        }
    }

    minio::s3::SetObjectTagsArgs objectTagsArgs;
    objectTagsArgs.bucket = bucket;
    objectTagsArgs.object = name;
    objectTagsArgs.tags[key] = value;

    minio::s3::SetObjectTagsResponse objectTagResponse = client.SetObjectTags(objectTagsArgs);
    if (!objectTagResponse) {
        qCDebug(lcMinioJob) << "unable to set tag for ↑↑↑";
        qCDebug(lcMinioJob) << QString::fromStdString(objectTagResponse.Error().String());
    }

    return objectTagResponse;
}

minio::s3::GetBucketTagsResponse MinioJob::setBucketTag(const std::string &bucket, const std::string &key, const std::string &value)
{
    auto creds = _account->credentials();
    qCDebug(lcMinioJob) << "user" << creds->user();
    qCDebug(lcMinioJob) << "bucket" << QString::fromStdString(bucket);
    qCDebug(lcMinioJob) << "key" << QString::fromStdString(key);
    qCDebug(lcMinioJob) << "value" << QString::fromStdString(value);

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    minio::s3::SetBucketTagsArgs bucketTagsArgs;
    bucketTagsArgs.bucket = bucket;
    bucketTagsArgs.tags[key] = value;

    minio::s3::SetBucketTagsResponse bucketTagResponse = client.SetBucketTags(bucketTagsArgs);

    return bucketTagResponse;
}

/**
 * @brief MinioJob::getOrUpdateAndGetRLMDatetime
 * Method gets current datetime and updates buckets or objects tag rlm_datetime
 * Tag rlm_datetime represents objects or "folders" real last modification
 * datetime. It updates objects up to the root node, including bucket.
 *
 * For example when folder1/folder2/file1.txt changes, then we set tag
 * rlm_datetime for following objects:
 * - folder1/folder2/file1.txt -> set tag {"rlm_datetime" = currentdatetime()}
 * - folder1/folder2/          -> set tag {"rlm_datetime" = currentdatetime()}
 * - folder1/                  -> set tag {"rlm_datetime" = currentdatetime()}
 *
 * With this "hack" clients can know when some "folder" was updated. It is
 * something simillar owncloud, ocis and cern are doing with their
 * implementation:
 * https://www.reddit.com/r/owncloud/comments/141vty4/ocis_and_external_storage/jn84v00/
 *
 * Even though this now costs us additional requests to an s3 storage we
 * blindly assume that somewhere in the future rlm_datetime or something
 * simillar will be implemented in ceph, aws, ... internally.
 *
 * Also NOTE that "rlm_datetime" sometimes would not be the same as datetime
 * of created object, since "rlm_datetime" was missing at the first place and
 * was added "much later".
 *
 * @param bucket
 * @param object name
 * @return std::string
 */
std::string MinioJob::getOrUpdateAndGetRLMDatetime(const std::string &bucket, const std::string &name) {
    auto currentTime = std::chrono::system_clock::now();
    std::time_t currentTimeT = std::chrono::system_clock::to_time_t(currentTime);
    std::tm* timeinfo = std::localtime(&currentTimeT);
    std::ostringstream oss;
    oss << std::put_time(timeinfo, "%Y:%m:%d %H:%M:%S");
    std::string formattedTime = oss.str();

    qCDebug(lcMinioJob) << "will try to get or update object tag with bucket" << QString::fromStdString(bucket) << "and name" << QString::fromStdString(name);

    std::string rlmDatetime = getObjectTag(bucket, name, "rlm_datetime");

    if (rlmDatetime != "") {
        return rlmDatetime;
    }

    minio::s3::SetObjectTagsResponse setObjectTagsResponse = setObjectTag(bucket, name, "rlm_datetime", formattedTime, true);
    if (setObjectTagsResponse) {
        rlmDatetime = formattedTime;
    }

    QStringList __path = QString::fromStdString(name).split(QStringLiteral("/"));
    qCDebug(lcMinioJob) << "doing path" << __path;

    __path.pop_back();

    qCDebug(lcMinioJob) << "path size after pop" << __path.size();

    while (__path.size() > 0) {
        QString tmpName = __path.join(QStringLiteral("/"));

        std::string tmpRlmDatetime = getObjectTag(bucket, tmpName.toStdString(), "rlm_datetime");

        if (tmpRlmDatetime == "") {
            minio::s3::SetObjectTagsResponse setObjectTagsResponse = setObjectTag(bucket, tmpName.toStdString() + "/", "rlm_datetime", formattedTime, true);
        }

        __path.pop_back();
    }

    std::string bucketRLMDatetime = getObjectTag(bucket, name, "rlm_datetime");

    if (bucketRLMDatetime == "") {
        minio::s3::SetBucketTagsResponse setObjectBucketResponse = setBucketTag(bucket, "rlm_datetime", formattedTime);
    }

    return rlmDatetime;
}

/**
 * @brief MinioJob::updateRLMDatetime
 * Method tries to update rlm_datetime no all nodes, no matter what. This is
 * used only when we upload the file.
 *
 * @param bucket
 * @param name
 * @return
 */
std::string MinioJob::updateRLMDatetime(const std::string &bucket, const std::string &name) {
    auto currentTime = std::chrono::system_clock::now();
    std::time_t currentTimeT = std::chrono::system_clock::to_time_t(currentTime);
    std::tm* timeinfo = std::localtime(&currentTimeT);
    std::ostringstream oss;
    oss << std::put_time(timeinfo, "%Y:%m:%d %H:%M:%S");
    std::string formattedTime = oss.str();

    qCDebug(lcMinioJob) << "will try to update object tag with bucket" << QString::fromStdString(bucket) << "and name" << QString::fromStdString(name);

    minio::s3::SetObjectTagsResponse setObjectTagsResponse = setObjectTag(bucket, name, "rlm_datetime", formattedTime, true);
    if (!setObjectTagsResponse) {
        return "";
    }

    QStringList __path = QString::fromStdString(name).split(QStringLiteral("/"));
    qCDebug(lcMinioJob) << "doing path" << __path;

    __path.pop_back();

    qCDebug(lcMinioJob) << "path size after pop" << __path.size();

    while (__path.size() > 0) {
        QString tmpName = __path.join(QStringLiteral("/"));

        minio::s3::SetObjectTagsResponse setObjectTagsResponse = setObjectTag(bucket, tmpName.toStdString() + "/", "rlm_datetime", formattedTime, true);
        if (!setObjectTagsResponse) {
            return "";
        }

        __path.pop_back();
    }

    minio::s3::SetBucketTagsResponse setObjectBucketResponse = setBucketTag(bucket, "rlm_datetime", formattedTime);
    if (!setObjectBucketResponse) {
        return "";
    }

    return formattedTime;
}

minio::s3::GetObjectTagsResponse MinioJob::getObjectTags(const std::string &bucket, const std::string &name)
{
    auto creds = _account->credentials();
    qCDebug(lcMinioJob) << "user" << creds->user();
    qCDebug(lcMinioJob) << "bucket" << QString::fromStdString(bucket);
    qCDebug(lcMinioJob) << "object" << QString::fromStdString(name);

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    minio::s3::GetObjectTagsArgs objectTagsArgs;
    objectTagsArgs.bucket = bucket;
    objectTagsArgs.object = name;

    minio::s3::GetObjectTagsResponse objectTagResponse = client.GetObjectTags(objectTagsArgs);

    return objectTagResponse;
}

void MinioJob::start()
{
    /*
     * if path == "/" -> list buckets
     * else list objects based on path
     */
    qCDebug(lcMinioJob) << "doing path" << path();

    QList<QString> bucketList;
    QStringList folders;

    auto creds = _account->credentials();

    qCDebug(lcMinioJob) << "user" << creds->user();

    minio::s3::BaseUrl base_url(_account->url().toString().toStdString());
    minio::creds::StaticProvider provider(creds->user().toStdString(), creds->password().toStdString());
    minio::s3::Client client(base_url, &provider);

    if (path() == QStringLiteral("/") || path() == QStringLiteral("")) {
        minio::s3::ListBucketsResponse resp = client.ListBuckets();
        if (!resp) {
            // todo: show error
            qCWarning(lcMinioJob) << "unable to do bucket existence check" << resp.Error();
        }

        for (const auto &bucket : resp.buckets) {
            folders.append(QString::fromUtf8(bucket.name.c_str()));
        }

        emit directoryListingSubfolders(folders);

        return;
    }

    std::string bucket;
    std::string prefix;
    const QStringList __path = path().split(QStringLiteral("/"));
    if (__path.size() >= 1) {
        bucket = __path[1].toStdString();
    }

    qCDebug(lcMinioJob) << "first element" << __path[1];
    qCDebug(lcMinioJob) << "path size" << __path.size();
    qCDebug(lcMinioJob) << "doing bucket" << QString::fromStdString(bucket);

    if (__path.size() > 2) {
        prefix = std::regex_replace(path().toStdString(), std::regex("^/" + bucket + "/"), "");
        prefix = std::regex_replace(prefix, std::regex("^/" + bucket), "");
        // prefix = std::regex_replace(prefix, std::regex("\\/\\/"), "");
    }

    qCDebug(lcMinioJob) << "doing prefix" << QString::fromStdString(prefix);

    minio::s3::ListObjectsArgs args;
    args.bucket = bucket;
    args.prefix = prefix;
    args.recursive = false;
    args.max_keys = 1000000; // todo Rok Jaklic hard limit for now

    minio::s3::ListObjectsResult result = client.ListObjects(args);

    for (; result; result++) {
        minio::s3::Item item = *result;
        if (item) {
            qCDebug(lcMinioJob) << "------------------";
            qCDebug(lcMinioJob) << "doing item";
            qCDebug(lcMinioJob) << "Name: " << QString::fromStdString(item.name);
            qCDebug(lcMinioJob) << "Version ID: " << QString::fromStdString(item.version_id);
            qCDebug(lcMinioJob) << "Size: " << item.size;
            qCDebug(lcMinioJob) << "Last Modified: " << item.last_modified.ToUTC();
            qCDebug(lcMinioJob) << "Delete Marker: " << minio::utils::BoolToString(item.is_delete_marker);

            qCDebug(lcMinioJob) << "User Metadata: ";
            for (auto &[key, value] : item.user_metadata) {
                qCDebug(lcMinioJob) << "  " << QString::fromStdString(key) << ": " << QString::fromStdString(value);
            }
            qCDebug(lcMinioJob) << "Owner ID: " << QString::fromStdString(item.owner_id);
            qCDebug(lcMinioJob) << "Owner Name: " << QString::fromStdString(item.owner_name);
            qCDebug(lcMinioJob) << "Storage Class: " << QString::fromStdString(item.storage_class);
            qCDebug(lcMinioJob) << "Is Latest: " << minio::utils::BoolToString(item.is_latest);
            qCDebug(lcMinioJob) << "Is Prefix: " << minio::utils::BoolToString(item.is_prefix);
            qCDebug(lcMinioJob) << "bucket name" << QString::fromStdString(item.bucket_name);
            qCDebug(lcMinioJob) << "object name" << QString::fromStdString(item.object_name);

            if (item.is_prefix && item.size == 0)  {
                QString fullPath = QStringLiteral("/") + QString::fromStdString(bucket) + QStringLiteral("/") + QString::fromStdString(prefix) + QString::fromUtf8(item.name.c_str());
                folders.append(fullPath);
            }

            // Here is a little hack. etag is actually an objects tag rlm_datetime.
            // Take a look into getOrUpdateAndGetRLMDatetime method description.
            item.etag = getOrUpdateAndGetRLMDatetime(bucket, item.name);

            item.name = "/" + bucket + "/" + item.name;
            qCDebug(lcMinioJob) << "item.name = bucket + item.name";
            qCDebug(lcMinioJob) << "item.name" << QString::fromStdString(item.name);

            qCDebug(lcMinioJob) << "ETag: " << QString::fromStdString(item.etag);
            qCDebug(lcMinioJob) << "------------------";

            QMap<QString, QString> currentHttp200Properties;
            emit directoryListingIteratedS3(item, currentHttp200Properties);
        } else {
            qCWarning(lcMinioJob) << "unable to listobjects" << item.Error();
            break;
        }
    }

    for (const auto& i : folders)
    {
        qCDebug(lcMinioJob) << "folder " << i;
        // emit directoryListingIterated(i, currentHttp200Properties);
    }    

    emit directoryListingSubfolders(folders);
    emit finishedWithoutError();
}

// TODO: Instead of doing all in this slot, we should iteratively parse in readyRead(). This
// would allow us to be more asynchronous in processing while data is coming from the network,
// not all in one big blob at the end.
void PropfindJob::finished()
{
    qCInfo(lcPropfindJob) << "LSCOL of" << reply()->request().url() << "FINISHED WITH STATUS"
                          << replyStatusString();

    QString contentType = reply()->header(QNetworkRequest::ContentTypeHeader).toString();
    int httpCode = reply()->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();
    if (httpCode == 207 && contentType.contains(QLatin1String("application/xml; charset=utf-8"))) {
        LsColXMLParser parser;
        connect(&parser, &LsColXMLParser::directoryListingSubfolders,
            this, &PropfindJob::directoryListingSubfolders);
        connect(&parser, &LsColXMLParser::directoryListingIterated,
            this, &PropfindJob::directoryListingIterated);
        connect(&parser, &LsColXMLParser::finishedWithError,
            this, &PropfindJob::finishedWithError);
        connect(&parser, &LsColXMLParser::finishedWithoutError,
            this, &PropfindJob::finishedWithoutError);
        if (_depth == Depth::Zero) {
            connect(&parser, &LsColXMLParser::directoryListingIterated, [&parser, counter = 0, this](const QString &name, const QMap<QString, QString> &) mutable {
                counter++;
                // With a depths of 0 we must receive only one listing
                if (OC_ENSURE(counter == 1)) {
                    disconnect(&parser, &LsColXMLParser::directoryListingIterated, this, &PropfindJob::directoryListingIterated);
                } else {
                    qCCritical(lcPropfindJob) << "Received superfluous directory listing for depth 0 propfind" << counter << "Path:" << name;
                }
            });
        }

        QString expectedPath = reply()->request().url().path(); // something like "/owncloud/remote.php/webdav/folder"
        if (!parser.parse(reply()->readAll(), &_sizes, expectedPath)) {
            // XML parse error
            emit finishedWithError(reply());
        }
    } else if (httpCode == 207) {
        // wrong content type
        emit finishedWithError(reply());
    } else {
        // wrong HTTP code or any other network error
        emit finishedWithError(reply());
    }
}

void MinioJob::finished()
{
}

const QHash<QString, qint64> &PropfindJob::sizes() const
{
    return _sizes;
}

const QHash<QString, qint64> &MinioJob::sizes() const
{
    return _sizes;
}
/*********************************************************************************************/

AvatarJob::AvatarJob(AccountPtr account, const QString &userId, int size, QObject *parent)
    : AbstractNetworkJob(account, account->url(), QStringLiteral("remote.php/dav/avatars/%1/%2.png").arg(userId, QString::number(size)), parent)
{
    setStoreInCache(true);
}

void AvatarJob::start()
{
    sendRequest("GET");
    AbstractNetworkJob::start();
}

QPixmap AvatarJob::makeCircularAvatar(const QPixmap &baseAvatar)
{
    int dim = baseAvatar.width();

    QPixmap avatar(dim, dim);
    avatar.fill(Qt::transparent);

    QPainter painter(&avatar);
    painter.setRenderHint(QPainter::Antialiasing);

    QPainterPath path;
    path.addEllipse(0, 0, dim, dim);
    painter.setClipPath(path);

    painter.drawPixmap(0, 0, baseAvatar);
    painter.end();

    return avatar;
}

void AvatarJob::finished()
{
    int http_result_code = reply()->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();

    QPixmap avImage;

    if (http_result_code == 200) {
        QByteArray pngData = reply()->readAll();
        if (pngData.size()) {
            if (avImage.loadFromData(pngData)) {
                qCDebug(lcAvatarJob) << "Retrieved Avatar pixmap!";
            }
        }
    }
    emit avatarPixmap(avImage);
}

/*********************************************************************************************/

EntityExistsJob::EntityExistsJob(AccountPtr account, const QUrl &rootUrl, const QString &path, QObject *parent)
    : AbstractNetworkJob(account, rootUrl, path, parent)
{
}

void EntityExistsJob::start()
{
    sendRequest("HEAD");
    AbstractNetworkJob::start();
}

void EntityExistsJob::finished()
{
    emit exists(reply());
}

/*********************************************************************************************/


DetermineAuthTypeJob::DetermineAuthTypeJob(AccountPtr account, QObject *parent)
    : AbstractNetworkJob(account, account->davUrl(), {}, parent)
{
    setAuthenticationJob(true);
    setForceIgnoreCredentialFailure(true);
}

void DetermineAuthTypeJob::start()
{
    qCInfo(lcDetermineAuthTypeJob) << "Determining auth type for" << url();

    QNetworkRequest req;
    // Prevent HttpCredentialsAccessManager from setting an Authorization header.
    req.setAttribute(HttpCredentials::DontAddCredentialsAttribute, true);
    // Don't reuse previous auth credentials
    req.setAttribute(QNetworkRequest::AuthenticationReuseAttribute, QNetworkRequest::Manual);
    sendRequest("PROPFIND", req);
    AbstractNetworkJob::start();
}

void DetermineAuthTypeJob::finished()
{
    auto authChallenge = reply()->rawHeader("WWW-Authenticate").toLower();
    auto result = AuthType::Basic;
    if (authChallenge.contains("bearer ")) {
        result = AuthType::OAuth;
    } else if (authChallenge.isEmpty()) {
        qCWarning(lcDetermineAuthTypeJob) << "Did not receive WWW-Authenticate reply to auth-test PROPFIND";
    }
    qCInfo(lcDetermineAuthTypeJob) << "Auth type for" << _account->davUrl() << "is" << result;
    emit this->authType(result);
}

SimpleNetworkJob::SimpleNetworkJob(AccountPtr account, const QUrl &rootUrl, const QString &path, const QByteArray &verb, const QNetworkRequest &req, QObject *parent)
    : AbstractNetworkJob(account, rootUrl, path, parent)
    , _request(req)
    , _verb(verb)
{
}

SimpleNetworkJob::SimpleNetworkJob(AccountPtr account, const QUrl &rootUrl, const QString &path, const QByteArray &verb, const UrlQuery &arguments, const QNetworkRequest &req, QObject *parent)
    : SimpleNetworkJob(account, rootUrl, path, verb, req, parent)
{
    Q_ASSERT((QList<QByteArray> { "GET", "PUT", "POST", "DELETE", "HEAD", "PATCH" }.contains(verb)));
    if (!arguments.isEmpty()) {
        QUrlQuery args;
        // ensure everything is percent encoded
        // this is especially important for parameters that contain spaces or +
        for (const auto &item : arguments) {
            args.addQueryItem(
                QString::fromUtf8(QUrl::toPercentEncoding(item.first)),
                QString::fromUtf8(QUrl::toPercentEncoding(item.second)));
        }
        if (verb == QByteArrayLiteral("POST") || verb == QByteArrayLiteral("PUT") || verb == QByteArrayLiteral("PATCH")) {
            _request.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/x-www-form-urlencoded; charset=UTF-8"));
            _body = args.query(QUrl::FullyEncoded).toUtf8();
            _device = new QBuffer(&_body);
        } else {
            setQuery(args);
        }
    }
}

SimpleNetworkJob::SimpleNetworkJob(AccountPtr account, const QUrl &rootUrl, const QString &path, const QByteArray &verb, const QJsonObject &arguments, const QNetworkRequest &req, QObject *parent)
    : SimpleNetworkJob(account, rootUrl, path, verb, QJsonDocument(arguments).toJson(), req, parent)
{
    _request.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/json"));
}

SimpleNetworkJob::SimpleNetworkJob(AccountPtr account, const QUrl &rootUrl, const QString &path, const QByteArray &verb, QIODevice *requestBody, const QNetworkRequest &req, QObject *parent)
    : SimpleNetworkJob(account, rootUrl, path, verb, req, parent)
{
    _device = requestBody;
}

SimpleNetworkJob::SimpleNetworkJob(AccountPtr account, const QUrl &rootUrl, const QString &path, const QByteArray &verb, QByteArray &&requestBody, const QNetworkRequest &req, QObject *parent)
    : SimpleNetworkJob(account, rootUrl, path, verb, new QBuffer(&_body), req, parent)
{
    _body = std::move(requestBody);
}

SimpleNetworkJob::~SimpleNetworkJob()
{
}
void SimpleNetworkJob::start()
{
    Q_ASSERT(!_verb.isEmpty());
    // AbstractNetworkJob will take ownership of the buffer
    sendRequest(_verb, _request, _device);
    AbstractNetworkJob::start();
}

void SimpleNetworkJob::addNewReplyHook(std::function<void(QNetworkReply *)> &&hook)
{
    _replyHooks.push_back(hook);
}

void SimpleNetworkJob::finished()
{
    if (_device) {
        _device->close();
    }
}

void SimpleNetworkJob::newReplyHook(QNetworkReply *reply)
{
    for (const auto &hook : _replyHooks) {
        hook(reply);
    }
}

void fetchPrivateLinkUrl(AccountPtr account, const QUrl &baseUrl, const QString &remotePath, QObject *target,
    const std::function<void(const QUrl &url)> &targetFun)
{
    if (account->capabilities().privateLinkPropertyAvailable()) {
        // Retrieve the new link by PROPFIND
        auto *job = new PropfindJob(account, baseUrl, remotePath, PropfindJob::Depth::Zero, target);
        job->setProperties({ QByteArrayLiteral("http://owncloud.org/ns:privatelink") });
        job->setTimeout(10s);
        QObject::connect(job, &PropfindJob::directoryListingIterated, target, [=](const QString &, const QMap<QString, QString> &result) {
            auto privateLinkUrl = result[QStringLiteral("privatelink")];
            if (!privateLinkUrl.isEmpty()) {
                targetFun(QUrl(privateLinkUrl));
            }
        });
        job->start();
    }
}

} // namespace OCC
