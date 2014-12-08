import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SongDataList {
    private List<SongData> songDataList = null;
    private float minDuration = Integer.MAX_VALUE;
    private float maxDuration = Integer.MIN_VALUE;

    public SongDataList() {
        this.songDataList = new ArrayList<SongData>();
    }

    public List<SongData> getSongDataList() {
        return songDataList;
    }

    public void add(final SongData songData) {
        this.songDataList.add(songData);
        if (songData.getDuration() < this.minDuration) {
            this.minDuration = songData.getDuration();
        }
        if (songData.getDuration() > this.maxDuration) {
            this.maxDuration = songData.getDuration();
        }
    }

    public Centroid getCentroid() {
        final SongData songDataSum = new SongData();

        for (final SongData songData : this.songDataList) {
            songDataSum.incrementData(songData, songDataList.size());
        }
        return new Centroid(songDataSum);
    }

    public float getMinDuration() {
        return minDuration;
    }
    public float getMaxDuration() {
        return maxDuration;
    }

    public int getSize() {
        return this.songDataList.size();
    }

    public void clearData() {
        this.songDataList.clear();
    }

    public Centroid getRandomSong() {
        Random randomizer = new Random();
        return new Centroid(this.songDataList.get(randomizer.nextInt(songDataList.size())));
    }
}
