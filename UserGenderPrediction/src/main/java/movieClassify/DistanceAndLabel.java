package movieClassify;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistanceAndLabel implements Writable {
    private double distance;
    private String label;

    public DistanceAndLabel(){

    }

    public DistanceAndLabel(double distance, String label) {
        this.distance = distance;
        this.label = label;
    }
    public void set(double distance, String label) {
        this.distance = distance;
        this.label = label;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(distance);
        dataOutput.writeUTF(label);
    }

    public void readFields(DataInput dataInput) throws IOException {
        distance = dataInput.readDouble();
        label = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return distance + "," +label;
    }
}
