package DMP_integration_with_CI360;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Class for reading Excel file with taxonomy
 */
public class CleverDataReader implements ReaderInterface {
    public XSSFWorkbook book;
    public AttributesTaxonomy attributesTaxonomy;
    public Map<String, Map<String, String>> dictionaryAttributesTaxonomy;

    /**
     * @param path This parameter is the path to the file with the taxonomy.
     */
    public CleverDataReader(String path) {
        this.book = openBook(path);
        this.attributesTaxonomy = getAttributesTaxonomy(book.getSheet("Attributes"));
        this.dictionaryAttributesTaxonomy = getDictionaryAttributesTaxonomy(book);
    }

    /**
     * Creates and returns an object to work with Excel file
     */
    public XSSFWorkbook openBook(final String path) {
        try {
            File file = new File(path);
            XSSFWorkbook book = (XSSFWorkbook) WorkbookFactory.create(file);

            return book;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (EncryptedDocumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * This method reads the attributes of the "Attributes" sheet taxonomy.
     *
     * @param sheet This parameter is an Excel file sheet.
     * @return Returns taxonomy attributes
     */
    public AttributesTaxonomy getAttributesTaxonomy(XSSFSheet sheet) {
        AttributesTaxonomy attributesTaxonomy = new AttributesTaxonomy();
        DataFormatter formatter = new DataFormatter();
        Iterator<Row> ri = sheet.rowIterator();
        while (ri.hasNext()) {
            XSSFRow row = (XSSFRow) ri.next();

            String cellValue = formatter.formatCellValue(row.getCell(1));
            if (row.getRowNum() < 3 || cellValue == "")
                continue;
            else {
                Iterator<Cell> ci = row.cellIterator();
                while (ci.hasNext()) {
                    XSSFCell cell = (XSSFCell) ci.next();

                    int columnIndex = cell.getColumnIndex();
                    if (columnIndex == 1)
                        attributesTaxonomy.ids.add(formatter.formatCellValue(cell));
                    else if (columnIndex == 2)
                        attributesTaxonomy.names.add(formatter.formatCellValue(cell));
                    else if (columnIndex == 3)
                        attributesTaxonomy.shortNames.add(formatter.formatCellValue(cell));
                    else if (columnIndex == 4)
                        attributesTaxonomy.types.add(formatter.formatCellValue(cell));
                    else if (columnIndex == 5)
                        attributesTaxonomy.descriptions.add(formatter.formatCellValue(cell));
                }
            }
        }

        return attributesTaxonomy;
    }

    /**
     * This method traverses the Excel file and reads records, forming a hashmap,
     * which is necessary for decrypting data.
     *
     * @param book This parameter is an object for working with Excel file.
     * @return Returns a hashmap involved in decrypting data.
     */
    public Map<String, Map<String, String>> getDictionaryAttributesTaxonomy(XSSFWorkbook book) {
        Map<String, Map<String, String>> sheetsAttributesTaxonomy = new HashMap<String, Map<String, String>>();
        List<String> continueSheetName = new ArrayList<String>(
                Arrays.asList(
                        new String[]{
                                "Id-Sync-Table",
                                "Attributes",
                                "Notes",
                                "Sheets",
                                "types",
                                "Groups",
                                "D-System"
                        }));

        DataFormatter formatter = new DataFormatter();
        Iterator<Sheet> si = book.sheetIterator();
        while (si.hasNext()) {
            XSSFSheet sheet = (XSSFSheet) si.next();

            String sheetName = sheet.getSheetName();
            if (continueSheetName.contains(sheetName))
                continue;

            sheetsAttributesTaxonomy.put(sheetName, new HashMap<String, String>());
            Map<String, String> idDescription = new HashMap<String, String>();
            Iterator<Row> ri = sheet.rowIterator();
            while (ri.hasNext()) {
                XSSFRow row = (XSSFRow) ri.next();

                String id = null, description = null;
                String cellValue = formatter.formatCellValue(row.getCell(1));
                if (row.getRowNum() < 3 || cellValue == "")
                    continue;
                else {
                    Iterator<Cell> ci = row.cellIterator();
                    while (ci.hasNext()) {
                        XSSFCell cell = (XSSFCell) ci.next();

                        int columnIndex = cell.getColumnIndex();
                        if (columnIndex == 1)
                            id = formatter.formatCellValue(cell);
                        else if (columnIndex == 3)
                            description = formatter.formatCellValue(cell);
                    }
                }

                idDescription.put(id, description);
            }

            sheetsAttributesTaxonomy.replace(sheetName, idDescription);
        }

        return sheetsAttributesTaxonomy;
    }
}
