package tudo.streamingrec.algorithms.helper;

/*
 * An interface for different SparseMatrix types.
 */

public interface SparseMatrix<T>{
	
	// Get dimensions
	public int getM();
	public int getN();
	public int getNumberOfEntries();
	// Gets the type
	public String getType();
	// Sets (i,j) to val
	public void set(int i, int j, T val);
	// Sets (i,j) to T's boolean interpretation of b
	public void setBool(int i, int j, boolean b);
	// Gets (i,j)
	public T get(int i, int j);
	// Gets (i,j) as T's boolean interpretation
	public boolean getBool(int i, int j);
	// Returns a copy
	public SparseMatrix<T> copy();
	// Gets (generic) row i of the matrix
	public Object getRow(int i);
}
