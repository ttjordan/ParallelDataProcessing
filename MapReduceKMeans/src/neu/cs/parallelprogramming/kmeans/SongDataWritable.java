package neu.cs.parallelprogramming.kmeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SongDataWritable implements Writable {
    private String artistID             = "";
    private float duration              = 0.0f;
    private String artistMbid           = "";
    private String artistMbtags         = "";
    private String artistFamiliarity    = "";
    private String artistHotness        = "";
    private String artistName           = "";
    private String artistLatitude       = "";
    private String artistLongitude      = "";
    private String artistLocation       = "";
    private String energy               = "";
    private String release              = "";
    private String loudness             = "";
    private String mode                 = "";
    private String songHotness          = "";
    private String songId               = "";
    private String tempo                = "";
    private String title                = "";
    private String trackID              = "";
    private String year                 = "";

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.artistID + "\n");
        dataOutput.writeFloat(this.duration);
        dataOutput.writeBytes(this.artistMbid + "\n");
        dataOutput.writeBytes(this.artistMbtags + "\n");
        dataOutput.writeBytes(this.artistFamiliarity + "\n");
        dataOutput.writeBytes(this.artistHotness + "\n");
        dataOutput.writeBytes(this.artistName + "\n");
        dataOutput.writeBytes(this.artistLatitude + "\n");
        dataOutput.writeBytes(this.artistLongitude + "\n");
        dataOutput.writeBytes(this.artistLocation + "\n");
        dataOutput.writeBytes(this.energy + "\n");
        dataOutput.writeBytes(this.release + "\n");
        dataOutput.writeBytes(this.loudness + "\n");
        dataOutput.writeBytes(this.mode + "\n");
        dataOutput.writeBytes(this.songHotness + "\n");
        dataOutput.writeBytes(this.songId + "\n");
        dataOutput.writeBytes(this.tempo + "\n");
        dataOutput.writeBytes(this.title + "\n");
        dataOutput.writeBytes(this.trackID + "\n");
        dataOutput.writeBytes(this.year + "\n");
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.artistID           = dataInput.readLine();
        this.duration           = dataInput.readFloat();
        this.artistMbid         = dataInput.readLine();
        this.artistMbtags       = dataInput.readLine();
        this.artistFamiliarity  = dataInput.readLine();
        this.artistHotness      = dataInput.readLine();
        this.artistName         = dataInput.readLine();
        this.artistLatitude     = dataInput.readLine();
        this.artistLongitude    = dataInput.readLine();
        this.artistLocation     = dataInput.readLine();
        this.energy             = dataInput.readLine();
        this.release            = dataInput.readLine();
        this.loudness           = dataInput.readLine();
        this.mode               = dataInput.readLine();
        this.songHotness        = dataInput.readLine();
        this.songId             = dataInput.readLine();
        this.tempo              = dataInput.readLine();
        this.title              = dataInput.readLine();
        this.trackID            = dataInput.readLine();
        this.year               = dataInput.readLine();
    }

    public String printData() {
        return this.trackID;
    }

    public void incrementData(final SongDataWritable songDataWritable, final int size) {
        this.duration += songDataWritable.getDuration()/size;
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

    public String getArtistMbid() {
        return artistMbid;
    }

    public void setArtistMbid(final String artistMbid) {
        this.artistMbid = artistMbid;
    }

    public String getArtistMbtags() {
        return artistMbtags;
    }

    public void setArtistMbtags(final String artistMbtags) {
        this.artistMbtags = artistMbtags;
    }

    public String getArtistFamiliarity() {
        return artistFamiliarity;
    }

    public void setArtistFamiliarity(final String artistFamiliarity) {
        this.artistFamiliarity = artistFamiliarity;
    }

    public String getArtistHotness() {
        return artistHotness;
    }

    public void setArtistHotness(final String artistHotness) {
        this.artistHotness = artistHotness;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(final String artistName) {
        this.artistName = artistName;
    }

    public String getArtistLatitude() {
        return artistLatitude;
    }

    public void setArtistLatitude(final String artistLatitude) {
        this.artistLatitude = artistLatitude;
    }

    public String getArtistLongitude() {
        return artistLongitude;
    }

    public void setArtistLongitude(final String artistLongitude) {
        this.artistLongitude = artistLongitude;
    }

    public String getArtistLocation() {
        return artistLocation;
    }

    public void setArtistLocation(final String artistLocation) {
        this.artistLocation = artistLocation;
    }

    public String getEnergy() {
        return energy;
    }

    public void setEnergy(final String energy) {
        this.energy = energy;
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(final String release) {
        this.release = release;
    }

    public String getLoudness() {
        return loudness;
    }

    public void setLoudness(final String loudness) {
        this.loudness = loudness;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(final String mode) {
        this.mode = mode;
    }

    public String getSongHotness() {
        return songHotness;
    }

    public void setSongHotness(final String songHotness) {
        this.songHotness = songHotness;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(final String songId) {
        this.songId = songId;
    }

    public String getTempo() {
        return tempo;
    }

    public void setTempo(final String tempo) {
        this.tempo = tempo;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(final String title) {
        this.title = title;
    }

    public String getTrackID() {
        return trackID;
    }

    public void setTrackID(final String trackID) {
        this.trackID = trackID;
    }

    public String getYear() {
        return year;
    }

    public void setYear(final String year) {
        this.year = year;
    }
}
