package media;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class SO2Writable implements Writable {

    private double so2;
    private int count;

    public SO2Writable() {
        // Constructor vacío requerido para la deserialización
    }

    public SO2Writable(double so2, int count) {
        this.so2 = so2;
        this.count = count;
    }

    public double getSO2() {
        return so2;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(so2);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        so2 = in.readDouble();
        count = in.readInt();
    }
}
