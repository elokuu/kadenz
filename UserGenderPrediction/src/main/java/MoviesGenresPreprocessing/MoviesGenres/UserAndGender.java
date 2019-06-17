package MoviesGenresPreprocessing.MoviesGenres;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserAndGender implements WritableComparable<UserAndGender> {
    String userID;
    int gender;
    int age;
    String occupation;
    String zip_code;

    public UserAndGender() {
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public String getZip_code() {
        return zip_code;
    }

    public void setZip_code(String zip_code) {
        this.zip_code = zip_code;
    }

    public int compareTo(UserAndGender o) {
        return this.getUserID().compareTo(o.getUserID());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userID);
        dataOutput.writeUTF(Integer.toString(gender));
        dataOutput.writeUTF(Integer.toString(age));
        dataOutput.writeUTF(occupation);
        dataOutput.writeUTF(zip_code);
    }

    public void readFields(DataInput dataInput) throws IOException {
        userID = dataInput.readUTF();
        gender = Integer.parseInt(dataInput.readUTF());
        age = Integer.parseInt(dataInput.readUTF());
        occupation = dataInput.readUTF();
        zip_code = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return userID+","+gender+","+age+","+occupation+","+zip_code;
    }
}
