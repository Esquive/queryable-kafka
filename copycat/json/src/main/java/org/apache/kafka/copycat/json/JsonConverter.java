/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.copycat.data.*;
import org.apache.kafka.copycat.errors.DataException;
import org.apache.kafka.copycat.storage.Converter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Implementation of Converter that uses JSON to store schemas and objects.
 */
public class JsonConverter implements Converter<JsonNode> {

    private static final HashMap<Schema.Type, JsonToCopycatTypeConverter> TO_COPYCAT_CONVERTERS = new HashMap<>();

    private static Object checkOptionalAndDefault(Schema schema) {
        if (schema.defaultValue() != null)
            return schema.defaultValue();
        if (schema.isOptional())
            return null;
        throw new DataException("Invalid null value for required field");
    }

    static {
        TO_COPYCAT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.booleanValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.INT8, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return (byte) value.intValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.INT16, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return (short) value.intValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.INT32, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.intValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.INT64, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.longValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.floatValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.doubleValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.BYTES, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                try {
                    if (value.isNull()) return checkOptionalAndDefault(schema);
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new DataException("Invalid bytes field", e);
                }
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.STRING, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.textValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.ARRAY, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);

                Schema elemSchema = schema.valueSchema();
                if (elemSchema == null)
                    throw new DataException("Array schema did not specify the element type");
                ArrayList<Object> result = new ArrayList<>();
                for (JsonNode elem : value) {
                    result.add(convertToCopycat(elemSchema, elem));
                }
                return result;
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.MAP, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);

                Schema keySchema = schema.keySchema();
                Schema valueSchema = schema.valueSchema();

                // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
                // primitive types or a complex type as a key, it will be encoded as a list of pairs
                Map<Object, Object> result = new HashMap<>();
                if (keySchema.type() == Schema.Type.STRING) {
                    if (!value.isObject())
                        throw new DataException("Map's with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                    while (fieldIt.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fieldIt.next();
                        result.put(entry.getKey(), convertToCopycat(valueSchema, entry.getValue()));
                    }
                } else {
                    if (!value.isArray())
                        throw new DataException("Map's with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    for (JsonNode entry : value) {
                        if (!entry.isArray())
                            throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                        if (entry.size() != 2)
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                        result.put(convertToCopycat(keySchema, entry.get(0)),
                                convertToCopycat(valueSchema, entry.get(1)));
                    }
                }
                return result;
            }
        });
        TO_COPYCAT_CONVERTERS.put(Schema.Type.STRUCT, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);

                if (!value.isObject())
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

                // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
                // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
                // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
                // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
                // just returns the schema Object and has no overhead.
                Struct result = new Struct(schema.schema());
                for (Field field : schema.fields())
                    result.put(field, convertToCopycat(field.schema(), value.get(field.name())));

                return result;
            }
        });

    }

    @Override
    public JsonNode fromCopycatData(Schema schema, Object value) {
        return convertToJsonWithSchemaEnvelope(schema, value);
    }

    @Override
    public SchemaAndValue toCopycatData(JsonNode value) {
        if (!value.isObject() || value.size() != 2 || !value.has(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME) || !value.has(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME))
            throw new DataException("JSON value converted to Copycat must be in envelope containing schema");

        Schema schema = asCopycatSchema(value.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        return new SchemaAndValue(schema, convertToCopycat(schema, value.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME)));
    }


    private static ObjectNode asJsonSchema(Schema schema) {
        final ObjectNode jsonSchema;
        switch (schema.type()) {
            case BOOLEAN:
                jsonSchema = JsonSchema.BOOLEAN_SCHEMA.deepCopy();
                break;
            case BYTES:
                jsonSchema = JsonSchema.BYTES_SCHEMA.deepCopy();
                break;
            case FLOAT64:
                jsonSchema = JsonSchema.DOUBLE_SCHEMA.deepCopy();
                break;
            case FLOAT32:
                jsonSchema = JsonSchema.FLOAT_SCHEMA.deepCopy();
                break;
            case INT8:
                jsonSchema = JsonSchema.INT8_SCHEMA.deepCopy();
                break;
            case INT16:
                jsonSchema = JsonSchema.INT16_SCHEMA.deepCopy();
                break;
            case INT32:
                jsonSchema = JsonSchema.INT32_SCHEMA.deepCopy();
                break;
            case INT64:
                jsonSchema = JsonSchema.INT64_SCHEMA.deepCopy();
                break;
            case STRING:
                jsonSchema = JsonSchema.STRING_SCHEMA.deepCopy();
                break;
            case ARRAY:
                jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.ARRAY_TYPE_NAME);
                jsonSchema.set(JsonSchema.ARRAY_ITEMS_FIELD_NAME, asJsonSchema(schema.valueSchema()));
                break;
            case MAP:
                jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.MAP_TYPE_NAME);
                jsonSchema.set(JsonSchema.MAP_KEY_FIELD_NAME, asJsonSchema(schema.keySchema()));
                jsonSchema.set(JsonSchema.MAP_VALUE_FIELD_NAME, asJsonSchema(schema.valueSchema()));
                break;
            case STRUCT:
                jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.STRUCT_TYPE_NAME);
                ArrayNode fields = JsonNodeFactory.instance.arrayNode();
                for (Field field : schema.fields()) {
                    ObjectNode fieldJsonSchema = asJsonSchema(field.schema());
                    fieldJsonSchema.put(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME, field.name());
                    fields.add(fieldJsonSchema);
                }
                jsonSchema.set(JsonSchema.STRUCT_FIELDS_FIELD_NAME, fields);
                break;
            default:
                throw new DataException("Couldn't translate unsupported schema type " + schema + ".");
        }

        jsonSchema.put(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME, schema.isOptional());
        if (schema.name() != null)
            jsonSchema.put(JsonSchema.SCHEMA_NAME_FIELD_NAME, schema.name());
        if (schema.version() != null)
            jsonSchema.put(JsonSchema.SCHEMA_VERSION_FIELD_NAME, schema.version());
        if (schema.doc() != null)
            jsonSchema.put(JsonSchema.SCHEMA_DOC_FIELD_NAME, schema.doc());
        if (schema.defaultValue() != null)
            jsonSchema.set(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME, convertToJson(schema, schema.defaultValue()));

        return jsonSchema;
    }


    private static Schema asCopycatSchema(JsonNode jsonSchema) {
        if (jsonSchema.isNull())
            return null;

        JsonNode schemaTypeNode = jsonSchema.get(JsonSchema.SCHEMA_TYPE_FIELD_NAME);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new DataException("Schema must contain 'type' field");

        final SchemaBuilder builder;
        switch (schemaTypeNode.textValue()) {
            case JsonSchema.BOOLEAN_TYPE_NAME:
                builder = SchemaBuilder.bool();
                break;
            case JsonSchema.INT8_TYPE_NAME:
                builder = SchemaBuilder.int8();
                break;
            case JsonSchema.INT16_TYPE_NAME:
                builder = SchemaBuilder.int16();
                break;
            case JsonSchema.INT32_TYPE_NAME:
                builder = SchemaBuilder.int32();
                break;
            case JsonSchema.INT64_TYPE_NAME:
                builder = SchemaBuilder.int64();
                break;
            case JsonSchema.FLOAT_TYPE_NAME:
                builder = SchemaBuilder.float32();
                break;
            case JsonSchema.DOUBLE_TYPE_NAME:
                builder = SchemaBuilder.float64();
                break;
            case JsonSchema.BYTES_TYPE_NAME:
                builder = SchemaBuilder.bytes();
                break;
            case JsonSchema.STRING_TYPE_NAME:
                builder = SchemaBuilder.string();
                break;
            case JsonSchema.ARRAY_TYPE_NAME:
                JsonNode elemSchema = jsonSchema.get(JsonSchema.ARRAY_ITEMS_FIELD_NAME);
                if (elemSchema == null)
                    throw new DataException("Array schema did not specify the element type");
                builder = SchemaBuilder.array(asCopycatSchema(elemSchema));
                break;
            case JsonSchema.MAP_TYPE_NAME:
                JsonNode keySchema = jsonSchema.get(JsonSchema.MAP_KEY_FIELD_NAME);
                if (keySchema == null)
                    throw new DataException("Map schema did not specify the key type");
                JsonNode valueSchema = jsonSchema.get(JsonSchema.MAP_VALUE_FIELD_NAME);
                if (valueSchema == null)
                    throw new DataException("Map schema did not specify the value type");
                builder = SchemaBuilder.map(asCopycatSchema(keySchema), asCopycatSchema(valueSchema));
                break;
            case JsonSchema.STRUCT_TYPE_NAME:
                builder = SchemaBuilder.struct();
                JsonNode fields = jsonSchema.get(JsonSchema.STRUCT_FIELDS_FIELD_NAME);
                if (fields == null || !fields.isArray())
                    throw new DataException("Struct schema's \"fields\" argument is not an array.");
                for (JsonNode field : fields) {
                    JsonNode jsonFieldName = field.get(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME);
                    if (jsonFieldName == null || !jsonFieldName.isTextual())
                        throw new DataException("Struct schema's field name not specified properly");
                    builder.field(jsonFieldName.asText(), asCopycatSchema(field));
                }
                break;
            default:
                throw new DataException("Unknown schema type: " + schemaTypeNode.textValue());
        }


        JsonNode schemaOptionalNode = jsonSchema.get(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME);
        if (schemaOptionalNode != null && schemaOptionalNode.isBoolean() && schemaOptionalNode.booleanValue())
            builder.optional();
        else
            builder.required();

        JsonNode schemaNameNode = jsonSchema.get(JsonSchema.SCHEMA_NAME_FIELD_NAME);
        if (schemaNameNode != null && schemaNameNode.isTextual())
            builder.name(schemaNameNode.textValue());

        JsonNode schemaVersionNode = jsonSchema.get(JsonSchema.SCHEMA_VERSION_FIELD_NAME);
        if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber()) {
            builder.version(schemaVersionNode.intValue());
        }

        JsonNode schemaDocNode = jsonSchema.get(JsonSchema.SCHEMA_DOC_FIELD_NAME);
        if (schemaDocNode != null && schemaDocNode.isTextual())
            builder.doc(schemaDocNode.textValue());

        JsonNode schemaDefaultNode = jsonSchema.get(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME);
        if (schemaDefaultNode != null)
            builder.defaultValue(convertToCopycat(builder, schemaDefaultNode));

        return builder.build();
    }


    /**
     * Convert this object, in org.apache.kafka.copycat.data format, into a JSON object with an envelope object
     * containing schema and payload fields.
     * @param schema the schema for the data
     * @param value the value
     * @return JsonNode-encoded version
     */
    private static JsonNode convertToJsonWithSchemaEnvelope(Schema schema, Object value) {
        return new JsonSchema.Envelope(asJsonSchema(schema), convertToJson(schema, value)).toJsonNode();
    }

    /**
     * Convert this object, in the org.apache.kafka.copycat.data format, into a JSON object, returning both the schema
     * and the converted object.
     */
    private static JsonNode convertToJson(Schema schema, Object value) {
        if (value == null) {
            if (schema.defaultValue() != null)
                return convertToJson(schema, schema.defaultValue());
            if (schema.isOptional())
                return JsonNodeFactory.instance.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        switch (schema.type()) {
            case INT8:
                return JsonNodeFactory.instance.numberNode((Byte) value);
            case INT16:
                return JsonNodeFactory.instance.numberNode((Short) value);
            case INT32:
                return JsonNodeFactory.instance.numberNode((Integer) value);
            case INT64:
                return JsonNodeFactory.instance.numberNode((Long) value);
            case FLOAT32:
                return JsonNodeFactory.instance.numberNode((Float) value);
            case FLOAT64:
                return JsonNodeFactory.instance.numberNode((Double) value);
            case BOOLEAN:
                return JsonNodeFactory.instance.booleanNode((Boolean) value);
            case STRING:
                CharSequence charSeq = (CharSequence) value;
                return JsonNodeFactory.instance.textNode(charSeq.toString());
            case BYTES:
                if (value instanceof byte[])
                    return JsonNodeFactory.instance.binaryNode((byte[]) value);
                else if (value instanceof ByteBuffer)
                    return JsonNodeFactory.instance.binaryNode(((ByteBuffer) value).array());
                else
                    throw new DataException("Invalid type for bytes type: " + value.getClass());
            case ARRAY: {
                if (!(value instanceof Collection))
                    throw new DataException("Invalid type for array type: " + value.getClass());
                Collection collection = (Collection) value;
                ArrayNode list = JsonNodeFactory.instance.arrayNode();
                for (Object elem : collection) {
                    JsonNode fieldValue = convertToJson(schema.valueSchema(), elem);
                    list.add(fieldValue);
                }
                return list;
            }
            case MAP: {
                if (!(value instanceof Map))
                    throw new DataException("Invalid type for array type: " + value.getClass());
                Map<?, ?> map = (Map<?, ?>) value;
                // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                boolean objectMode = schema.keySchema().type() == Schema.Type.STRING;
                ObjectNode obj = null;
                ArrayNode list = null;
                if (objectMode)
                    obj = JsonNodeFactory.instance.objectNode();
                else
                    list = JsonNodeFactory.instance.arrayNode();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    JsonNode mapKey = convertToJson(schema.keySchema(), entry.getKey());
                    JsonNode mapValue = convertToJson(schema.valueSchema(), entry.getValue());

                    if (objectMode)
                        obj.set(mapKey.asText(), mapValue);
                    else
                        list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
                }
                return objectMode ? obj : list;
            }
            case STRUCT: {
                if (!(value instanceof Struct))
                    throw new DataException("Invalid type for struct type: " + value.getClass());
                Struct struct = (Struct) value;
                if (struct.schema() != schema)
                    throw new DataException("Mismatching schema.");
                ObjectNode obj = JsonNodeFactory.instance.objectNode();
                for (Field field : schema.fields()) {
                    obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
                }
                return obj;
            }
        }

        throw new DataException("Couldn't convert " + value + " to JSON.");
    }


    private static Object convertToCopycat(Schema schema, JsonNode jsonValue) {
        JsonToCopycatTypeConverter typeConverter = TO_COPYCAT_CONVERTERS.get(schema.type());
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + schema.type());

        return typeConverter.convert(schema, jsonValue);
    }


    private interface JsonToCopycatTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }
}