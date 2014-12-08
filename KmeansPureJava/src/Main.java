import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Main {
    private final static String DATAFILE = "/Users/tjordan/Desktop/pprog/project/AllData.csv";

    public static void main(String[] args) {
        SongDataList songDataList = null;
        final int NUMBEROFCLUSTERS = 6;
        try {
            songDataList = DataParser.getData(DATAFILE);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        KMeans.initialize(songDataList, NUMBEROFCLUSTERS);
        KMeans.run();
        try {
            KMeans.printResults();
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }
}
