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
import org.apache.mahout.classifier.ConfusionMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import utility.BinUtil;

public class HistoricTrendPredictor extends HistoricMeanPredictor {

    private static final String NAME = "Historic Trend Value Predictor";

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

    /**
     * Classifica il vettore e ritorna il valore della variabile di uscita
     * (intero tra 0 e {@link AbstractPredictor#getNumategories()}-1).
     *
     * @param vector vettore da classificare. Il vettore deve avere dimensione 5
     * e rappresenta la transazione. Esso contiene (in ordine): Classifica il
     * vettore e ritorna il valore della variabile di uscita (intero tra 0 e
     * {@link AbstractPredictor#getNumategories()}-1). Il vettore deve avere
     * dimensione 5 e rappresenta la transazione. Esso contiene (in ordine):
     * <li>bike_station_id</li>
     * <li>daily_bin_id</li>
     * <li>available_bike_average</li>
     * <li>bike_station_size</li>
     * <li>category_id</li>
     * Il <i>bike_station_id</i> è utilizzato per recuperare il corretto
     * predittore.<br>
     * <li>category_id</li> non è utilizzato.
     *
     * @return il valore della variabile di predire o -1 in caso di errore
     */
    @Override
    public int classify(Vector vector) {
        int stationId = (int) vector.get(0);
        int t0 = (int) vector.get(BIN_ID_INDEX_TRANSACTION);
        double historicMean[] = stationHistoricMean.get(stationId);
        int Bt0 = (int)vector.get(vector.size()-1);
        double BTBt0 = Math.round(historicMean[t0 % historicMean.length]);
        double BTBt0pw = Math.round(historicMean[(t0+pw) % historicMean.length]);
        int classified = (int) (Bt0 + BTBt0pw - BTBt0);
        if(classified < 0) classified = 0;
        if(classified >= getNumCategories()) classified = getNumCategories()-1;
        return classified;
    }
}
