import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class KMeans {
    private static final int MAXITER = 10000;
    private static List<Cluster> clusterList = null;
    private static SongDataList songDataList = null;
    private static float totalDistanceToPreviousIter = 0;
    private static float newTotalDistanceToPreviousIter = 0;
    private final static float THRESHOLD = 4;
    private static int numberOfClusters = -1;

    private static void assignDataToClusters() {
        int clusterId;
        int minDistanceCluster;
        float minDistance;
        for (final SongData songData : songDataList.getSongDataList()) {
            minDistance = Float.MAX_VALUE;
            minDistanceCluster = -1;
            clusterId = 0;
            for (final Cluster cluster : clusterList) {
                final float distanceCandidate = cluster.getDistanceTocluster(songData);
                if (minDistance > distanceCandidate) {
                    minDistance = distanceCandidate;
                    minDistanceCluster = clusterId;
                }
                clusterId++;
            }
            clusterList.get(minDistanceCluster).add(songData, minDistance);
        }
    }

    public static void run() {
        int counter = 0;
        boolean done = false;


        while ((counter < MAXITER) & !done) {
            System.out.println("Iteration " + counter);
            //clear data points assignment
            for (final Cluster cluster : clusterList) {
                cluster.reset();
            }
            assignDataToClusters();

            newTotalDistanceToPreviousIter = 0;
            for (final Cluster cluster : clusterList) {
                newTotalDistanceToPreviousIter += cluster.getSpreadFromCentroid();
            }

            if (counter > 0) {
                System.out.println("distance is " + Math.abs(totalDistanceToPreviousIter - newTotalDistanceToPreviousIter));
                if (Math.abs(totalDistanceToPreviousIter - newTotalDistanceToPreviousIter) < THRESHOLD) {
                    done = true;
                }
            }
            counter++;
            totalDistanceToPreviousIter = newTotalDistanceToPreviousIter;

            for (final Cluster cluster : clusterList) {
                cluster.updateCentroid();
            }
        }
    }

    public static void initialize(final SongDataList newSongDataList, final int k) {
        System.out.println("song dataset has " + newSongDataList.getSize() + " songs");
        numberOfClusters = k;
        clusterList = new ArrayList<Cluster>();
        songDataList = newSongDataList;

        for (int i = 0; i < numberOfClusters; i++) {
            clusterList.add(new Cluster(i, newSongDataList.getRandomSong()));
        }
    }

    public static void printResults() throws FileNotFoundException, UnsupportedEncodingException {
        System.out.println("There are " + numberOfClusters + " clusters");

        final PrintWriter writer = new PrintWriter("clusters.txt", "UTF-8");

        for (final Cluster cluster : clusterList) {
            System.out.println("Cluster size is " + cluster.getSize());
            System.out.println("Cluster min value is " + cluster.getMin());
            System.out.println("Cluster max value is " + cluster.getMax());
            System.out.println("Centroid is " + cluster.getCentroid().getDuration());
            cluster.writeResults(writer);
        }
        System.out.println("Clusters spread is " + newTotalDistanceToPreviousIter);
        writer.close();
    }
}
