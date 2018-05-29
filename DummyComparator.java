import java.io.Serializable;
import java.util.Comparator;

public class DummyComparator implements Serializable, Comparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
        return o1.compareTo(o2);
    }
}