const DEFAULT_MESSAGE_SIZE_THRESHOLD = 256000;

function getMessageAttributesSize(messageAttributes) {
    if (!messageAttributes) {
        return 0;
    }

    let size = 0;

    Object.keys(messageAttributes).forEach((attrKey) => {
        const attr = messageAttributes[attrKey];

        size += Buffer.byteLength(attrKey, 'utf8');
        size += Buffer.byteLength(attr.DataType, 'utf8');
        size +=
            typeof attr.StringValue !== 'undefined' && attr.StringValue !== null
                ? Buffer.byteLength(attr.StringValue, 'utf8')
                : 0;
        size +=
            typeof attr.BinaryValue !== 'undefined' && attr.BinaryValue !== null
                ? Buffer.byteLength(attr.BinaryValue, 'utf8')
                : 0;
    });

    return size;
}

function getMessageSize(message) {
    const messageAttributeSize = getMessageAttributesSize(message.MessageAttributes);
    const bodySize = Buffer.byteLength(message.MessageBody, 'utf8');
    return messageAttributeSize + bodySize;
}

function isLarge(message, messageSizeThreshold = DEFAULT_MESSAGE_SIZE_THRESHOLD) {
    return getMessageSize(message) > messageSizeThreshold;
}

module.exports = {
    DEFAULT_MESSAGE_SIZE_THRESHOLD,
    getMessageAttributesSize,
    getMessageSize,
    isLarge,
};
