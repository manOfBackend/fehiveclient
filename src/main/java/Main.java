import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

public class Main {


    public static void main(String args[]) throws IOException, CsvValidationException {

        CSVReader csvReader = new CSVReader(new FileReader("test_1GB.csv"));
        CSVWriter csvWriter = new CSVWriter(new FileWriter("output_1GB.csv"));

        csvReader.skip(1);
        Iterator<String[]> iterator = csvReader.iterator();
        while (iterator.hasNext()) {

            String[] line = iterator.next();
            for (int i = 0; i < line.length; i++) {
                if (line[i].isEmpty()) {
                    line[i] = "0";
                }
            }
            csvWriter.writeNext(line);

        }
        csvWriter.flush();
        csvWriter.close();
        csvReader.close();


    }

}
