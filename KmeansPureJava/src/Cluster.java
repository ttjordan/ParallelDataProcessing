import java.io.PrintWriter;

public class Cluster {
    private int clusterID;
    private Centroid centroid = null;
    private SongDataList songDataList = null;
    float spreadFromCentroid =  0;

    public Cluster(final int clusterID, final Centroid centroid) {
        this.clusterID = clusterID;
        this.songDataList = new SongDataList();
        this.centroid = centroid;
    }

    public double getSpreadFromCentroid() {
        return spreadFromCentroid;
    }

    public int getSize() {
        return this.songDataList.getSize();
    }

    public float getDistanceTocluster(final SongData songData) {
        return (float) Math.sqrt(Math.pow(songData.getDuration() - centroid.getDuration(), 2));
    }

    public void add(final SongData songData, final float distanceFromCentroid) {
        this.songDataList.add(songData);
        this.spreadFromCentroid += distanceFromCentroid;
    }

    public float getMin() {
        return this.songDataList.getMinDuration();
    }

    public float getMax() {
        return this.songDataList.getMaxDuration();
    }

    public void updateCentroid() {
        this.centroid = this.songDataList.getCentroid();
    }

    public Centroid getCentroid() {
        return centroid;
    }

    public void setCentroid(Centroid centroid) {
        this.centroid = centroid;
    }

    public void reset(){
        songDataList.clearData();
        spreadFromCentroid = 0;
    }

    public void writeResults(final PrintWriter printWriter) {
        for (final SongData songData : this.songDataList.getSongDataList()) {
            printWriter.println(songData.getArtistID()                  + "," +
                                songData.getArtistMbid()                + "," +
                                songData.getArtistMbtags()              + "," +
                                songData.getArtistFamiliarity()         + "," +
                                songData.getArtistHotness()             + "," +
                                songData.getArtistName()                + "," +
                                songData.getArtistLatitude()            + "," +
                                songData.getArtistLongitude()           + "," +
                                songData.getArtistLocation()            + "," +
                                songData.getDuration()                  + "," +
                                songData.getEnergy()                    + "," +
                                songData.getRelease()                   + "," +
                                songData.getLoudness()                  + "," +
                                songData.getMode()                      + "," +
                                songData.getSongHotness()               + "," +
                                songData.getSongId()                    + "," +
                                songData.getTempo()                     + "," +
                                songData.getTitle()                     + "," +
                                songData.getTrackID()                   + "," +
                                songData.getYear()                      + "," +
                                this.clusterID);
        }
    }
}
