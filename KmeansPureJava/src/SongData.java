
public class SongData {
    private String artistID;
    private float duration;
    private String artistMbid;
    private String artistMbtags;
    private String artistFamiliarity;
    private String artistHotness;
    private String artistName;
    private String artistLatitude;
    private String artistLongitude;
    private String artistLocation;
    private String energy;
    private String release;
    private String loudness;
    private String mode;
    private String songHotness;
    private String songId;
    private String tempo;
    private String title;
    private String trackID;
    private String year;

    public SongData() {
        this.artistID = "";
        this.duration = 0;
    }

    public String printData() {
        return this.artistID + "," + this.duration + "," + this.artistMbid + "," + this.artistMbtags + ","
                + this.artistFamiliarity + "," + this.artistHotness + "," + this.artistName + "," + this.artistLatitude + "," + this.artistLongitude + ","
                + this.energy + "," + this.release + "," + this.loudness + "," + this.mode + "," + this.songHotness + "," + this.songId + "," + this.tempo
                + "," + this.title + "," + this.trackID + "," + year + "\n";
    }

    public String getArtistID() {
        return artistID;
    }

    public void setArtistID(final String artistID) {
        this.artistID = artistID;
    }

    public float getDuration() {
        return duration;
    }

    public void setDuration(final float duration) {
        this.duration = duration;
    }

    public void incrementData(final SongData songData, final float number) {

        this.duration += songData.getDuration()/number;
    }

    public String getArtistMbid() {
        return artistMbid;
    }

    public void setArtistMbid(String artistMbid) {
        this.artistMbid = artistMbid;
    }


    public String getArtistMbtags() {
        return artistMbtags;
    }

    public void setArtistMbtags(String artistMbtags) {
        this.artistMbtags = artistMbtags;
    }

    public String getArtistFamiliarity() {
        return artistFamiliarity;
    }

    public void setArtistFamiliarity(String artistFamiliarity) {
        this.artistFamiliarity = artistFamiliarity;
    }

    public String getArtistHotness() {
        return artistHotness;
    }

    public void setArtistHotness(String artistHotness) {
        this.artistHotness = artistHotness;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getArtistLatitude() {
        return artistLatitude;
    }

    public void setArtistLatitude(String artistLatitude) {
        this.artistLatitude = artistLatitude;
    }

    public String getArtistLongitude() {
        return artistLongitude;
    }

    public void setArtistLongitude(String artistLongitude) {
        this.artistLongitude = artistLongitude;
    }

    public String getArtistLocation() {
        return artistLocation;
    }

    public void setArtistLocation(String artistLocation) {
        this.artistLocation = artistLocation;
    }

    public String getEnergy() {
        return energy;
    }

    public void setEnergy(String energy) {
        this.energy = energy;
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public String getLoudness() {
        return loudness;
    }

    public void setLoudness(String loudness) {
        this.loudness = loudness;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getSongHotness() {
        return songHotness;
    }

    public void setSongHotness(String songHotness) {
        this.songHotness = songHotness;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public String getTempo() {
        return tempo;
    }

    public void setTempo(String tempo) {
        this.tempo = tempo;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTrackID() {
        return trackID;
    }

    public void setTrackID(String trackID) {
        this.trackID = trackID;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }
}
