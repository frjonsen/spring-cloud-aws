/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.support.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Fredrik Jonsén
 * @since 3.2.0
 */
public class EventBridgeMessageConverter extends WrappedMessageConverter {
	private static final String WRITING_CONVERSION_ERROR = "This converter only supports reading an EventBridge message and not writing them";

	public EventBridgeMessageConverter(MessageConverter payloadConverter, ObjectMapper jsonMapper) {
		super(jsonMapper, payloadConverter);
	}

	@Override
	protected Object fromGenericMessage(GenericMessage<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
		JsonNode jsonNode;
		try {
			jsonNode = this.jsonMapper.readTree(message.getPayload().toString());
		}
		catch (Exception e) {
			throw new MessageConversionException("Could not read JSON", e);
		}

		if (!jsonNode.has("detail")) {
			throw new MessageConversionException(
					"Payload: '" + message.getPayload() + "' does not contain a detail attribute", null);
		}

		// Unlike SNS, where the message is a nested JSON string, the message payload in EventBridge is a JSON object
		JsonNode messagePayload = jsonNode.get("detail");
		GenericMessage<JsonNode> genericMessage = new GenericMessage<>(messagePayload);
		Object convertedMessage;
		if (payloadConverter instanceof SmartMessageConverter) {
			convertedMessage = ((SmartMessageConverter) payloadConverter).fromMessage(genericMessage, targetClass, conversionHint);
		} else {
			convertedMessage = payloadConverter.fromMessage(genericMessage, targetClass);
		}

		return convertedMessage;
	}

	@Override
	protected String getWritingConversionError() {
		return WRITING_CONVERSION_ERROR;
	}
}
