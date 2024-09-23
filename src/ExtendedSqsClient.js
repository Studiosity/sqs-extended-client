/* eslint-disable no-underscore-dangle */
const { v4: uuidv4 } = require('uuid');
const { getMessageSize, DEFAULT_MESSAGE_SIZE_THRESHOLD } = require('./sqsMessageSizeUtils');

const MESSAGE_POINTER_CLASS = 'com.amazon.sqs.javamessaging.MessageS3Pointer'
const RECEIPT_HANDLER_MATCHER = /^-\.\.s3BucketName\.\.-(.*)-\.\.s3BucketName\.\.--\.\.s3Key\.\.-(.*)-\.\.s3Key\.\.-(.*)/;
const RESERVED_ATTRIBUTE_NAME = 'SQSLargePayloadSize';
const S3_BUCKET_NAME_MARKER = '-..s3BucketName..-';
const S3_KEY_MARKER = '-..s3Key..-';

function getS3MessageKeyAndBucket(message) {
    const messageAttributes = message.messageAttributes || message.MessageAttributes || {};

    if (!messageAttributes[RESERVED_ATTRIBUTE_NAME]) {
        return {
            bucketName: null,
            s3MessageKey: null,
        };
    }

    const payload = JSON.parse(message.body || message.Body);
    if (Array.isArray(payload) && payload.length === 2 && payload[0] === MESSAGE_POINTER_CLASS) {
        return {
            bucketName: payload[1]['s3BucketName'],
            s3MessageKey: payload[1]['s3Key']
        };
    } else {
        return {
            bucketName: null,
            s3MessageKey: null,
        };
    }
}

function embedS3MarkersInReceiptHandle(bucketName, s3MessageKey, receiptHandle) {
    return `${S3_BUCKET_NAME_MARKER}${bucketName}${S3_BUCKET_NAME_MARKER}${S3_KEY_MARKER}${s3MessageKey}${S3_KEY_MARKER}${receiptHandle}`;
}

function extractBucketNameFromReceiptHandle(receiptHandle) {
    const match = receiptHandle.match(RECEIPT_HANDLER_MATCHER);
    return match ? match[1] : null;
}

function extractS3MessageKeyFromReceiptHandle(receiptHandle) {
    const match = receiptHandle.match(RECEIPT_HANDLER_MATCHER);
    return match ? match[2] : null;
}

function getOriginalReceiptHandle(receiptHandle) {
    const match = receiptHandle.match(RECEIPT_HANDLER_MATCHER);
    return match ? match[3] : receiptHandle;
}

function addMessageSizeAttribute(messageSize, attributes) {
    return {
        ...attributes,
        [RESERVED_ATTRIBUTE_NAME]: {
            DataType: 'Number',
            StringValue: messageSize.toString(),
        },
    };
}

function wrapRequest(request, callback, sendFn) {
    if (callback) {
        sendFn(callback);
    }

    return {
        ...request,
        send: sendFn,
        promise: sendFn,
    };
}

function invokeFnBeforeRequest(request, fn) {
    return (callback) =>
        new Promise((resolve, reject) => {
            fn()
                .then(() => {
                    request
                        .promise()
                        .then((response) => {
                            if (callback) {
                                callback(undefined, response);
                            }

                            resolve(response);
                        })
                        .catch((err) => {
                            if (callback) {
                                callback(err);
                                resolve();
                                return;
                            }

                            reject(err);
                        });
                })
                .catch((fnErr) => {
                    if (callback) {
                        callback(fnErr);
                        resolve();
                        return;
                    }

                    reject(fnErr);
                });
        });
}

function invokeFnAfterRequest(request, fn) {
    return (callback) =>
        new Promise((resolve, reject) => {
            request
                .promise()
                .then((response) => {
                    fn(response)
                        .then(() => {
                            if (callback) {
                                callback(undefined, response);
                            }

                            resolve(response);
                        })
                        .catch((s3Err) => {
                            if (callback) {
                                callback(s3Err);
                                resolve();
                                return;
                            }

                            reject(s3Err);
                        });
                })
                .catch((err) => {
                    if (callback) {
                        callback(err);
                        resolve();
                        return;
                    }

                    reject(err);
                });
        });
}

class ExtendedSqsClient {
    constructor(sqs, s3, options = {}) {
        this.sqs = sqs;
        this.s3 = s3;
        this.bucketName = options.bucketName;
        this.alwaysUseS3 = options.alwaysUseS3;
        this.messageSizeThreshold = options.messageSizeThreshold || DEFAULT_MESSAGE_SIZE_THRESHOLD;
    }

    _storeS3Content(key, s3Content) {
        const params = {
            Bucket: this.bucketName,
            Key: key,
            Body: s3Content,
        };

        return this.s3.putObject(params).promise();
    }

    async _getS3Content(bucketName, key) {
        const params = {
            Bucket: bucketName,
            Key: key,
        };

        const object = await this.s3.getObject(params).promise();
        return object.Body.toString();
    }

    _deleteS3Content(bucketName, key) {
        const params = {
            Bucket: bucketName,
            Key: key,
        };

        return this.s3.deleteObject(params).promise();
    }

    changeMessageVisibility(params, callback) {
        return this.sqs.changeMessageVisibility(
            {
                ...params,
                ReceiptHandle: getOriginalReceiptHandle(params.ReceiptHandle),
            },
            callback
        );
    }

    changeMessageVisibilityBatch(params, callback) {
        return this.sqs.changeMessageVisibilityBatch(
            {
                ...params,
                Entries: params.Entries.map((entry) => ({
                    ...entry,
                    ReceiptHandle: getOriginalReceiptHandle(entry.ReceiptHandle),
                })),
            },
            callback
        );
    }

    /* eslint-disable-next-line class-methods-use-this */
    _prepareDelete(params) {
        return {
            bucketName: extractBucketNameFromReceiptHandle(params.ReceiptHandle),
            s3MessageKey: extractS3MessageKeyFromReceiptHandle(params.ReceiptHandle),
            deleteParams: {
                ...params,
                ReceiptHandle: getOriginalReceiptHandle(params.ReceiptHandle),
            },
        };
    }

    deleteMessage(params, callback) {
        const { bucketName, s3MessageKey, deleteParams } = this._prepareDelete(params);

        if (!s3MessageKey) {
            return this.sqs.deleteMessage(deleteParams, callback);
        }

        const request = this.sqs.deleteMessage(deleteParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () => this._deleteS3Content(bucketName, s3MessageKey))
        );
    }

    deleteMessageBatch(params, callback) {
        const entryObjs = params.Entries.map((entry) => this._prepareDelete(entry));

        const deleteParams = { ...params };
        deleteParams.Entries = entryObjs.map((entryObj) => entryObj.deleteParams);

        const request = this.sqs.deleteMessageBatch(deleteParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () =>
                Promise.all(
                    entryObjs.map(({ bucketName, s3MessageKey }) => {
                        if (s3MessageKey) {
                            return this._deleteS3Content(bucketName, s3MessageKey);
                        }
                        return Promise.resolve();
                    })
                )
            )
        );
    }

    _prepareSend(params) {
        const sendParams = { ...params };
        const messageSize = getMessageSize(sendParams);
        console.log(`ESC#_prepareSend - alwaysUseS3: ${this.alwaysUseS3}, large message?: ${messageSize > this.messageSizeThreshold}`);
        const useS3 = this.alwaysUseS3 || messageSize > this.messageSizeThreshold;
        const s3Content = useS3 ? sendParams.MessageBody : null;
        let s3MessageKey;

        if (s3Content) {
            s3MessageKey = uuidv4();
            sendParams.MessageAttributes = addMessageSizeAttribute(messageSize, sendParams.MessageAttributes);
            sendParams.MessageBody = JSON.stringify(
              [
                  MESSAGE_POINTER_CLASS,
                  { s3BucketName: this.bucketName, s3Key: s3MessageKey }
              ]
            );
            console.log(`ESC#_prepareSend - S3 key: ${s3MessageKey}`);
        }

        return {
            s3MessageKey,
            sendParams,
            s3Content,
        };
    }

    sendMessage(params, callback) {
        if (!this.bucketName) {
            throw new Error('bucketName option is required for sending messages');
        }

        const { s3MessageKey, sendParams, s3Content } = this._prepareSend(params);

        if (!s3MessageKey) {
            console.log(`ESC#sendMessage - No S3 key, sending body via SQS (size: ${Buffer.byteLength(params.MessageBody, 'utf8')})`);
            return this.sqs.sendMessage(sendParams, callback);
        }

        const request = this.sqs.sendMessage(sendParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () => this._storeS3Content(s3MessageKey, s3Content))
        );
    }

    sendMessageBatch(params, callback) {
        if (!this.bucketName) {
            throw new Error('bucketName option is required for sending messages');
        }

        const entryObjs = params.Entries.map((entry) => this._prepareSend(entry));

        const sendParams = { ...params };
        sendParams.Entries = entryObjs.map((entryObj) => entryObj.sendParams);

        const request = this.sqs.sendMessageBatch(sendParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () =>
                Promise.all(
                    entryObjs.map(({ s3Content, s3MessageKey }) => {
                        if (s3MessageKey) {
                            return this._storeS3Content(s3MessageKey, s3Content);
                        }

                        return Promise.resolve();
                    })
                )
            )
        );
    }

    _processReceive() {
        return (response) =>
            Promise.all(
                (response.Messages || []).map(async (message) => {
                    const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(message);

                    if (s3MessageKey) {
                        /* eslint-disable-next-line no-param-reassign */
                        message.Body = await this._getS3Content(bucketName, s3MessageKey);
                        /* eslint-disable-next-line no-param-reassign */
                        message.ReceiptHandle = embedS3MarkersInReceiptHandle(
                            bucketName,
                            s3MessageKey,
                            message.ReceiptHandle
                        );
                    }
                })
            );
    }

    receiveMessage(params, callback) {
        const modifiedParams = {
            ...params,
            MessageAttributeNames: [...(params.MessageAttributeNames || []), RESERVED_ATTRIBUTE_NAME],
        };

        const request = this.sqs.receiveMessage(modifiedParams);
        return wrapRequest(request, callback, invokeFnAfterRequest(request, this._processReceive()));
    }
}

module.exports = ExtendedSqsClient;
