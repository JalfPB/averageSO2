package media;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SO2AverageDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SO2Job <inputPath> <outputPath>");
            System.exit(-1);
        }

        // Configuración del job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SO2 Job");

        // Clases de la aplicación
        job.setJarByClass(SO2AverageDriver.class);
        job.setMapperClass(SO2Mapper.class);
        job.setReducerClass(SO2Reducer.class);

        // Tipos de salida
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); // Cambiado a Text para el Reducer

        // Rutas de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Ejecutar el job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


