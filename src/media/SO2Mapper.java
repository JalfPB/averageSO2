package media;

import java.io.IOException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SO2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final String SEPARATOR = ";";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        try {
            final String so2 = values[9];
            final String province = values[10];

            if (NumberUtils.isNumber(so2)) {
                // Cambia Text a DoubleWritable para la clave
                context.write(new Text(province), new DoubleWritable(NumberUtils.toDouble(so2)));
            } else {
                System.err.println("Valor no numérico para SO2 en la fila: " + value);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Error al procesar la fila: " + value);
        }
    }
}





