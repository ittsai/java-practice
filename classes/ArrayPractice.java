import java.util.AbstractList;
import java.util.Collection;

public class ArrayPractice <T> extends AbstractList<T> {
    T[] a; // The array used to store elements
    int n; // The number of elements stored

    public static void main(String[] args) {

    }

    @Override
    public T get(int index) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    // more efficient way, like normal addAll, once we add a element based on the idx,
    // we need to shift the elements, with this method, first we make the space and then
    // add all element one time.
    public void addAll(int index, Collection<? extends T> collection) {
        if (index < 0 || index > n) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + n);
        }
        int collectionSize = collection.size();

        if (n + collectionSize > a.length) {
            // doing resize
        }

        for (int i = 0; i < n - index; i++) {
            a[index + collectionSize + i] = a[index + i];
        }

        int currentIndex = index;
        for(T element: collection) {
            a[currentIndex] = element;
            currentIndex++;
        }

        n+=collectionSize;
    }
}
