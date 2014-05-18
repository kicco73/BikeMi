package prediction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import utility.BinUtil;

public class HistoricTrendPredictor extends AbstractPredictor {

    private static final String NAME = "Historic Trend Value Predictor";
    /**
     * Indice del vettore delle transazioni in cui è memorizzato il binId
     */

    private final int BIN_ID_INDEX_TRANSACTION = 1;

    /**
     * Costruttore
     *
     * @param numCategories numero di valori che la variabile di uscita può
     * assumere
     * @param pw finestra di predizione
     */
    public HistoricTrendPredictor(int numCategories, int pw) {
        super(numCategories, pw);
    }

    @Override
    public int classify(Vector vector) {
        // TODO Implement me for the project
        return 0;
    }

    /**
     * Trasforma una lista di stringhe in un vettore compatibile con Mahout
     *
     * @param data lista da trasformare
     * @return vector vettore che contiene gli stessi elementi della lista allo
     * stesso indice
     */
    private Vector getVectorFromDataTransaction(List<String> data) {
        Vector vec = new DenseVector(data.size());
        int i = 0;
        for (String value : data) {
            vec.set(i++, Double.parseDouble(value));
        }
        return vec;
    }

    /**
     * Ordina la lista per ogni bike station in base al bin id.
     *
     * @param data map da ordinare
     */
    private void sort(HashMap<Integer, List<Vector>> data) {
        // Define the comparator
        Comparator<Vector> comparator = new Comparator<Vector>() {
            @Override
            public int compare(Vector o1, Vector o2) {
                return new Double(o1.get(BIN_ID_INDEX_TRANSACTION)).compareTo(new Double(o2.get(BIN_ID_INDEX_TRANSACTION)));
            }

        };

        // For each bike station
        for (Map.Entry<Integer, List<Vector>> entry : data.entrySet()) {
            int id = entry.getKey();
            List<Vector> currentVectorId = entry.getValue();
            Collections.sort(currentVectorId, comparator); // sort the list
            data.put(id, currentVectorId);
        }
    }

    @Override
    public void buildClassifier(Path input, String pattern, int days,
            int binsPerDay, int numTarget)
            throws IOException {

        input = input.suffix("/part-r-00000");

        Pattern splitter = Pattern.compile(pattern);

        // Holds Features for the training set
        HashMap<Integer, List<Vector>> dataTraining = new HashMap<Integer, List<Vector>>();
        // Holds Features for the test set
        HashMap<Integer, List<Vector>> dataTest = new HashMap<Integer, List<Vector>>();

		////////////////////////////////////////////////////////
        // 		 Reads data and set the parameters 			  //
        ////////////////////////////////////////////////////////
        Configuration conf = new Configuration();
        // Recupero il filesystem utilizzato
        FileSystem fs = FileSystem.get(input.toUri(), conf);
        // Read data
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, input, conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)) {
            String[] values = splitter.split(value.toString());
            int dayId = key.get();
            List<String> transaction = null;
            int bikeId = Integer.parseInt(values[0]); // get the id of the bike station
            if (dayId == days - 1) { // Test set
                List<Vector> vectorBikeIdList = dataTest.get(bikeId);
                if (vectorBikeIdList == null) {
                    vectorBikeIdList = new ArrayList<Vector>();
                }

                transaction = new ArrayList<String>(new ArrayList<String>(Arrays.asList(values)));
                vectorBikeIdList.add(getVectorFromDataTransaction(transaction));
                dataTest.put(bikeId, vectorBikeIdList); // add transaction to the test set
            } else { // Training set
                // get list of index
                List<Vector> vectorBikeIdList = dataTraining.get(bikeId);
                if (vectorBikeIdList == null) {
                    vectorBikeIdList = new ArrayList<Vector>();
                }

                values[1] = "" + BinUtil.getUniqueBinIdInAllDays(dayId, binsPerDay, Integer.parseInt(values[1]));
                transaction = new ArrayList<String>(new ArrayList<String>(Arrays.asList(values)));;
                vectorBikeIdList.add(getVectorFromDataTransaction(transaction));
                dataTraining.put(bikeId, vectorBikeIdList); // add transaction to the training set
            }

        }

        reader.close();

        sort(dataTraining); // sort the training set
        train(dataTraining, binsPerDay); // train the models

        sort(dataTest); // sort the test set
        evaluate(dataTest); // test the model
    }

    @Override
    public String printInfo() {
        StringBuilder sb = new StringBuilder(NAME + ":\n");
        sb.append(super.printInfo());
        return sb.toString();
    }

    private void train(HashMap<Integer, List<Vector>> dataTraining, int binsPerDay) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void evaluate(HashMap<Integer, List<Vector>> dataTest) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
