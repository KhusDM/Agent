package DMP_integration_with_CI360;

import java.util.ArrayList;
import java.util.List;

/**
 * The entity class of the "Attributes" sheet of the file with the taxonomy CleverData.
 * The class contains attributes: ids - attribute ids, names - attribute names, shortNames - short attribute names,
 * types - attribute types (CleverData has two attribute types: significant (int, string, etc.)
 * and referential (refer to taxonomy sheets)), descriptions - attribute descriptions
 */
public class AttributesTaxonomy {
    public List<String> ids = new ArrayList<String>();
    public List<String> names = new ArrayList<String>();
    public List<String> shortNames = new ArrayList<String>();
    public List<String> types = new ArrayList<String>();
    public List<String> descriptions = new ArrayList<String>();
}
