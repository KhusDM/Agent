package DMP_integration_with_CI360;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.Map;

/**
 * Interface that implements reader class
 */
public interface ReaderInterface {
    XSSFWorkbook openBook(final String path);

    Map<String, Map<String, String>> getDictionaryAttributesTaxonomy(XSSFWorkbook book);
}
