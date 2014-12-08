
public class Centroid {
    private float duration = 0;

    public Centroid(final SongData songData) {
        this.duration = songData.getDuration();
    }

    public float getDuration() {
        return duration;
    }
}
