import au.com.bytecode.opencsv.CSVParser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataParser {

    public static SongDataList getData(final String file) throws IOException {

        final CSVParser csvParser = new CSVParser();
        final SongDataList songDataList = new SongDataList();

        final FileInputStream fis = new FileInputStream(file);

        //Construct BufferedReader from InputStreamReader
        final BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line;
        while ((line = br.readLine()) != null) {
            final String[] lineWithSongInfo   = csvParser.parseLine(line);
            final SongData songData = new SongData();
            songData.setArtistID(lineWithSongInfo[0]);
            songData.setArtistMbid(lineWithSongInfo[1]);
            songData.setArtistMbtags(lineWithSongInfo[2]);
            songData.setArtistFamiliarity(lineWithSongInfo[3]);
            songData.setArtistHotness(lineWithSongInfo[4]);
            songData.setArtistName(lineWithSongInfo[5]);
            songData.setArtistLatitude(lineWithSongInfo[6]);
            songData.setArtistLongitude(lineWithSongInfo[7]);
            songData.setArtistLocation(lineWithSongInfo[8]);
            songData.setDuration(Float.parseFloat(lineWithSongInfo[9]));
            songData.setEnergy(lineWithSongInfo[10]);
            songData.setRelease(lineWithSongInfo[11]);
            songData.setLoudness(lineWithSongInfo[12]);
            songData.setMode(lineWithSongInfo[13]);
            songData.setSongHotness(lineWithSongInfo[14]);
            songData.setSongId(lineWithSongInfo[15]);
            songData.setTempo(lineWithSongInfo[16]);
            songData.setTitle(lineWithSongInfo[17]);
            songData.setTrackID(lineWithSongInfo[18]);
            songData.setYear(lineWithSongInfo[19]);
            songDataList.add(songData);
        }

        br.close();

        return songDataList;
    }
}
