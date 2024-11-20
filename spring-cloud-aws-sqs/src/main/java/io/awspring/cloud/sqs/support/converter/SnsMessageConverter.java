/*
 * Copyright 2013-2023 the original author or authors.
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
 * @author Michael Sosa
 * @author gustavomonarin
 * @author Wei Jiang
 * @since 3.1.1
 */
public class SnsMessageConverter extends WrappedMessageConverter {
	private static final String WRITING_CONVERSION_ERROR = "This converter only supports reading a SNS notification and not writing them";

	public SnsMessageConverter(MessageConverter payloadConverter, ObjectMapper jsonMapper) {
		super(jsonMapper, payloadConverter);
	}

	protected Object fromGenericMessage(GenericMessage<?> message, Class<?> targetClass,
			@Nullable Object conversionHint) {
		JsonNode jsonNode;
		try {
			jsonNode = this.jsonMapper.readTree(message.getPayload().toString());
		}
		catch (Exception e) {
			throw new MessageConversionException("Could not read JSON", e);
		}
		if (!jsonNode.has("Type")) {
			throw new MessageConversionException(
					"Payload: '" + message.getPayload() + "' does not contain a Type attribute", null);
		}

		if (!"Notification".equals(jsonNode.get("Type").asText())) {
			throw new MessageConversionException("Payload: '" + message.getPayload() + "' is not a valid notification",
					null);
		}

		if (!jsonNode.has("Message")) {
			throw new MessageConversionException("Payload: '" + message.getPayload() + "' does not contain a message",
					null);
		}

		String messagePayload = jsonNode.get("Message").asText();
		GenericMessage<String> genericMessage = new GenericMessage<>(messagePayload);
		Object convertedMessage = (payloadConverter instanceof SmartMessageConverter)
				? ((SmartMessageConverter) this.payloadConverter).fromMessage(genericMessage, targetClass,
						conversionHint)
				: this.payloadConverter.fromMessage(genericMessage, targetClass);
		return convertedMessage;
	}

	@Override
	protected String getWritingConversionError() {
		return WRITING_CONVERSION_ERROR;
	}
}
