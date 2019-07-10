package DMP_integration_with_CI360;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.*;

/**
 * This class is for decrypting data.
 */
public class CleverDataConverter {
    public AttributesTaxonomy attributesTaxonomy;
    public Map<String, Map<String, String>> dictionaryAttributesTaxonomy;
    private List<String> valueTypes = new ArrayList<>(Arrays.asList(new String[]{
            "Boolean", "Character", "Byte", "Integer",
            "Long", "Double", "String", "Enum", "ABoolean",
            "ACharacter", "AByte", "AInteger", "ALong", "ADouble",
            "AString", "AEnum", "AAByte", "D-Currency"
    }));

    public CleverDataConverter(AttributesTaxonomy attributesTaxonomy, Map<String, Map<String, String>> dictionaryAttributesTaxonomy) {
        this.attributesTaxonomy = attributesTaxonomy;
        this.dictionaryAttributesTaxonomy = dictionaryAttributesTaxonomy;
    }

    /**
     * This method traverses the attributes of the encrypted data and uses a hashmap to decrypt
     *
     * @param codedDataJsonObject This parameter is a json object that contains encrypted data.
     * @return Returns a json object with decrypted data.
     */
    public JsonObject getConvertedCookieData(JsonObject codedDataJsonObject) {
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        Map<String, String> convertedAttributesData = new HashMap<String, String>();
        for (JsonElement codedAttributeData : codedDataJsonObject.get("attrs").getAsJsonArray()) {
            JsonObject codedAttribute = codedAttributeData.getAsJsonObject();
            if (attributesTaxonomy.ids.contains(codedAttribute.get("primary").getAsString())) {
                int index = attributesTaxonomy.ids.indexOf(codedAttribute.get("primary").getAsString());
                String attributeName = attributesTaxonomy.names.get(index), attributeType = attributesTaxonomy.types.get(index);

                String attributeValue = null;
                if (!valueTypes.contains(attributeType))
                    try {
                        attributeValue = dictionaryAttributesTaxonomy.get(attributeType).get(codedAttribute.get("secondary").getAsString());
                    } catch (Exception ex) {
                        System.out.println("Error!");
                    }
                else {
                    attributeValue = codedAttribute.get("secondary").getAsString();
                }

                if (attributeValue != null && !attributeValue.isEmpty() && !attributeValue.equals("Отсутствует") && !attributeValue.equals("0"))
                    convertedAttributesData.put(attributeName, attributeValue);
            }
        }

        JsonObject convertedDataJsonObject = new JsonObject();
        convertedDataJsonObject.addProperty("id", codedDataJsonObject.get("id").getAsString());
        convertedDataJsonObject.add("attrs", parser.parse(gson.toJson(convertedAttributesData)).getAsJsonObject());

        return convertedDataJsonObject;
    }
}
