const ExtendedSqsClient = require('../ExtendedSqsClient');

const mockS3Key = '1234-5678';

jest.mock('uuid', () => ({
    v4: () => mockS3Key,
}));

const testMessageAttribute = {
    DataType: 'String',
    StringValue: 'attr value',
};
const messageSizeKeyAttribute = {
    DataType: 'Number',
    StringValue: '5242880',
};

describe('ExtendedSqsClient sendMessage', () => {
    it('should send small message directly', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'small message body',
            })
            .promise();

        // Then
        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: 'small message body',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
        });
    });

    it('should use S3 to send large message (using promise())', async () => {
        // Given
        const sqsResponse = { messageId: 'test message id' };

        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve(sqsResponse) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: largeMessageBody,
            })
            .promise();

        // Then
        expect(result).toEqual(sqsResponse);

        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                SQSLargePayloadSize: {
                    DataType: "Number",
                    StringValue: "4194336",
                },
            },
        });
    });

    it('should use S3 to send large message (using callback)', async () => {
        // Given
        const sqsResponse = { messageId: 'test message id' };

        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve(sqsResponse) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await new Promise((resolve, reject) => {
            const callback = (err, data) => (err ? reject(err) : resolve(data));

            client.sendMessage(
                {
                    QueueUrl: 'test-queue',
                    MessageAttributes: { MessageAttribute: testMessageAttribute },
                    MessageBody: largeMessageBody,
                },
                callback
            );
        });

        // Then
        expect(result).toEqual(sqsResponse);

        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                SQSLargePayloadSize: {
                    DataType: "Number",
                    StringValue: "4194336",
                },
            },
        });
    });

    it('should use S3 to send large message (using send() callback)', async () => {
        // Given
        const sqsResponse = { messageId: 'test message id' };

        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve(sqsResponse) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await new Promise((resolve, reject) => {
            const request = client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: largeMessageBody,
            });

            request.send((err, data) => (err ? reject(err) : resolve(data)));
        });

        // Then
        expect(result).toEqual(sqsResponse);

        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                SQSLargePayloadSize: {
                    DataType: "Number",
                    StringValue: "4194336"
                },
            },
        });
    });

    it('should use s3 to send small message when alwaysUseS3=true', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'small message body',
            })
            .promise();

        // Then
        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: 'small message body',
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                SQSLargePayloadSize: {
                    DataType: "Number",
                    StringValue: "50",
                },
            },
        });
    });

    it('should ignore existing SQSLargePayloadSize', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { SQSLargePayloadSize: messageSizeKeyAttribute },
                MessageBody: 'message body',
            })
            .promise();

        // Then
        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
            MessageAttributes: {
                SQSLargePayloadSize: {
                    DataType: "Number",
                    StringValue: "44"
                },
            },
        });

        expect(s3.putObject).toHaveBeenCalledTimes(1);
    });

    it('should throw SQS error (using promise())', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .sendMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributes: { MessageAttribute: testMessageAttribute },
                    MessageBody: 'body'.repeat(1024 * 1024),
                })
                .promise()
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw SQS error (using callback)', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.sendMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributes: { MessageAttribute: testMessageAttribute },
                        MessageBody: 'body'.repeat(1024 * 1024),
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using promise())', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({})),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .sendMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributes: { MessageAttribute: testMessageAttribute },
                    MessageBody: 'body'.repeat(1024 * 1024),
                })
                .promise();
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using callback)', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({})),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.sendMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributes: { MessageAttribute: testMessageAttribute },
                        MessageBody: 'body'.repeat(1024 * 1024),
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });
});

describe('ExtendedSqsClient sendMessageBatch', () => {
    it('should batch send messages', async () => {
        // Given
        const sqs = {
            sendMessageBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        const largeMessageBody1 = 'body1'.repeat(1024 * 1024);
        const largeMessageBody2 = 'body2'.repeat(1024 * 1024);

        // When
        await client
            .sendMessageBatch({
                QueueUrl: 'test-queue',
                Entries: [
                    {
                        Id: '1',
                        MessageBody: largeMessageBody1,
                    },
                    {
                        Id: '2',
                        MessageBody: 'small message body',
                    },
                    {
                        Id: '3',
                        MessageBody: largeMessageBody2,
                    },
                ],
            })
            .promise();

        // Then
        expect(s3.putObject).toHaveBeenCalledTimes(2);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: '1234-5678',
            Body: largeMessageBody1,
        });
        expect(s3.putObject.mock.calls[1][0]).toEqual({
            Bucket: 'test-bucket',
            Key: '1234-5678',
            Body: largeMessageBody2,
        });

        expect(sqs.sendMessageBatch).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessageBatch.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
                    MessageAttributes: { SQSLargePayloadSize: messageSizeKeyAttribute },
                },
                {
                    Id: '2',
                    MessageBody: 'small message body',
                },
                {
                    Id: '3',
                    MessageBody: "[\"com.amazon.sqs.javamessaging.MessageS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"1234-5678\"}]",
                    MessageAttributes: { SQSLargePayloadSize: messageSizeKeyAttribute },
                },
            ],
        });
    });
});

describe('ExtendedSqsClient receiveMessage', () => {
    it('should receive a message not using S3', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandler: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const client = new ExtendedSqsClient(sqs, {});

        // When
        const response = await client
            .receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            })
            .promise();

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'SQSLargePayloadSize'],
            QueueUrl: 'test-queue',
        });

        expect(response).toEqual(messages);
    });

    it('should receive a message using S3 (using promise())', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: JSON.stringify([
                        "com.amazon.sqs.javamessaging.MessageS3Pointer",
                        { s3BucketName: 'test-bucket', s3Key: mockS3Key }
                    ]),
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const response = await client
            .receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            })
            .promise();

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'SQSLargePayloadSize'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(response).toEqual({
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        });
    });

    it('should receive a message using S3 (using callback)', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: JSON.stringify([
                        "com.amazon.sqs.javamessaging.MessageS3Pointer",
                        { s3BucketName: 'test-bucket', s3Key: mockS3Key }
                    ]),
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const response = await new Promise((resolve, reject) => {
            const callback = (err, data) => (err ? reject(err) : resolve(data));

            client.receiveMessage(
                {
                    QueueUrl: 'test-queue',
                    MessageAttributeNames: ['MessageAttribute'],
                },
                callback
            );
        });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'SQSLargePayloadSize'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(response).toEqual({
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        });
    });

    it('should receive a message using S3 (using send() callback)', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: JSON.stringify([
                        "com.amazon.sqs.javamessaging.MessageS3Pointer",
                        { s3BucketName: 'test-bucket', s3Key: mockS3Key }
                    ]),
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const response = await new Promise((resolve, reject) => {
            const callback = (err, data) => (err ? reject(err) : resolve(data));

            const request = client.receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            });

            request.send(callback);
        });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'SQSLargePayloadSize'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(response).toEqual({
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        });
    });

    it('should throw SQS error (using promise())', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            getObject: jest.fn(() => ({})),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .receiveMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributeNames: ['MessageAttribute'],
                })
                .promise();
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw SQS error (using callback)', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            getObject: jest.fn(() => ({})),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.receiveMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributeNames: ['MessageAttribute'],
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using promise())', async () => {
        // Given
        // Given
        const messages = {
            Messages: [
                {
                    Body: JSON.stringify([
                        "com.amazon.sqs.javamessaging.MessageS3Pointer",
                        { s3BucketName: 'test-bucket', s3Key: '8765-4321' }
                    ]),
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .receiveMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributeNames: ['MessageAttribute'],
                })
                .promise();
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using callback)', async () => {
        // Given
        // Given
        const messages = {
            Messages: [
                {
                    Body: JSON.stringify([
                        "com.amazon.sqs.javamessaging.MessageS3Pointer",
                        { s3BucketName: 'test-bucket', s3Key: '8765-4321' }
                    ]),
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        SQSLargePayloadSize: messageSizeKeyAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.receiveMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributeNames: ['MessageAttribute'],
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });
});

describe('ExtendedSqsClient deleteMessage', () => {
    it('should delete a message not using S3', async () => {
        // Given
        const sqs = {
            deleteMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .deleteMessage({
                QueueUrl: 'test-queue',
                ReceiptHandle: 'receipthandle',
            })
            .promise();

        // Then
        expect(sqs.deleteMessage).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it('should delete a message and S3 message content', async () => {
        // Given
        const sqs = {
            deleteMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            deleteObject: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        await client
            .deleteMessage({
                QueueUrl: 'test-queue',
                ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
            })
            .promise();

        // Then
        expect(sqs.deleteMessage).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });

        expect(s3.deleteObject).toHaveBeenCalledTimes(1);
        expect(s3.deleteObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });
    });
});

describe('ExtendedSqsClient deleteMessageBatch', () => {
    it('should batch delete messages', async () => {
        // Given
        const sqs = {
            deleteMessageBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            deleteObject: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        await client
            .deleteMessageBatch({
                QueueUrl: 'test-queue',
                Entries: [
                    {
                        Id: '1',
                        ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}1-..s3Key..-receipthandle1`,
                    },
                    {
                        Id: '2',
                        ReceiptHandle: 'receipthandle2',
                    },
                    {
                        Id: '1',
                        ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}2-..s3Key..-receipthandle3`,
                    },
                ],
            })
            .promise();

        // Then
        expect(sqs.deleteMessageBatch).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessageBatch.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: `receipthandle1`,
                },
                {
                    Id: '2',
                    ReceiptHandle: 'receipthandle2',
                },
                {
                    Id: '1',
                    ReceiptHandle: `receipthandle3`,
                },
            ],
        });

        expect(s3.deleteObject).toHaveBeenCalledTimes(2);
        expect(s3.deleteObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: `${mockS3Key}1`,
        });
        expect(s3.deleteObject.mock.calls[1][0]).toEqual({
            Bucket: 'test-bucket',
            Key: `${mockS3Key}2`,
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibility', () => {
    it('should change visibility of a message not using S3', async () => {
        // Given
        const sqs = {
            changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .changeMessageVisibility({
                QueueUrl: 'test-queue',
                ReceiptHandle: 'receipthandle',
            })
            .promise();

        // Then
        expect(sqs.changeMessageVisibility).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibility.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it('should change visibility of a message using S3', async () => {
        // Given
        const sqs = {
            changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .changeMessageVisibility({
                QueueUrl: 'test-queue',
                ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
            })
            .promise();

        // Then
        expect(sqs.changeMessageVisibility).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibility.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibilityBatch', () => {
    it('should batch change visibility', async () => {
        // Given
        const sqs = {
            changeMessageVisibilityBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .changeMessageVisibilityBatch({
                QueueUrl: 'test-queue',
                Entries: [
                    {
                        Id: '1',
                        ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle1`,
                    },
                    {
                        Id: '2',
                        ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle2`,
                    },
                ],
            })
            .promise();

        // Then
        expect(sqs.changeMessageVisibilityBatch).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibilityBatch.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: `receipthandle1`,
                },
                {
                    Id: '2',
                    ReceiptHandle: `receipthandle2`,
                },
            ],
        });
    });
});
