package tudo.streamingrec.algorithms.helper;

import java.io.Serializable;

import gnu.trove.map.TIntByteMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntByteProcedure;
import gnu.trove.procedure.TIntObjectProcedure;

/**
 * A class that represents a n*m Matrix with byte values optimized for space with sparse data.
 * Not set values are always 0
 * 
 * @author LL
 */

public class SparseByteMatrix implements Serializable, SparseMatrix<Byte>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1968296592755890929L;
	TIntObjectMap<TIntByteMap> matrix;
	
	//dimensions, number of non 0 entries
	private int M, N, numberOfEntries;
	
	/**
	 * Creates a new matrix; the matrix has no capacity bounds.
	 */
	public SparseByteMatrix(){
		matrix = new TIntObjectHashMap<TIntByteMap>();
		numberOfEntries = 0;
	}
	
	/**
	 * Creates a new matrix; the matrix has no capacity bounds.
	 * @param m -
	 * @param n -
	 */
	public SparseByteMatrix(int m, int n){
		matrix = new TIntObjectHashMap<TIntByteMap>();
		M = m;
		N = n;
		numberOfEntries = 0;
	}
	
	/**
	 * Set the element in row i, column j to value b.
	 * @param i row index
	 * @param j column index
	 * @param b value
	 */
	public void set(int i, int j, Byte b){
		TIntByteMap row = matrix.get(i);
		if (row == null){
			// Every not set entry is implicitly 0
			// don't waste space creating a new row
			if (b == 0) return;
//			System.out.println("new row and not b=0.");
//			System.out.println("not extist.");
			row = new TIntByteHashMap();
			matrix.put(i, row);
		}
		// Every not set entry is implicitly 0
		if (b == 0){
			// don't waste space inserting a 0
			if (row.get(j) == 0) return;
			// if an entry was already there
			// remove it instead of writing a 0
			row.remove(j);
			numberOfEntries--;
		} else {
			if (row.get(j) == 0) numberOfEntries++; 
			row.put(j, b);
		}
		
	}
	
	/**
	 * Sets the value of the matrix boolean-style.
	 * @param i row index
	 * @param j column index
	 * @param b boolean value
	 */
	public void setBool(int i, int j, boolean b){
		if (b) set(i,j,(byte)1);
		else set(i,j,(byte)0);
	}
	
	/**
	 * Gets the value at row i, column j; not yet set values are 0
	 * @param i -
	 * @param j -
	 * @return the value in the matrix
	 */
	public Byte get(int i, int j){
		TIntByteMap row = matrix.get(i);
		if (row != null){
			byte value = row.get(j);
			return value;
		}
//		System.out.println("get not ex");
		return 0;
	}
	
	/**
	 * Gets the value at row i, column j as boolean
	 * @param i -
	 * @param j -
	 * @return the value in the matrix as a boolean
	 */
	public boolean getBool(int i, int j){
		TIntByteMap row = matrix.get(i);
		if (row != null){
			byte value = row.get(j);
			return value!=0;
		}
//		System.out.println("get not ex");
		return false;
	}
	
	/**
	 * Gets the i'th column
	 * @param i -
	 * @return the values in the i'th column
	 */
	public TIntByteMap getRow(int i){
		return matrix.get(i);
	}
	
	/**
	 * Returns an independent copy of the SparseMatrix
	 * @return a copy of the object
	 */
	public SparseByteMatrix copy(){
		SparseByteMatrix copy = new SparseByteMatrix();
		OuterRunner outerRunner = new OuterRunner(copy);
		matrix.forEachEntry(outerRunner);
		return copy;
	}
	
	/**
	 * Needed for copying.
	 */
	private class OuterRunner implements TIntObjectProcedure<TIntByteMap>{
		SparseByteMatrix copy;
		OuterRunner(SparseByteMatrix copy){
			this.copy = copy;
		}
		@Override
		public boolean execute(int i, TIntByteMap row) {
			InnerRunner innerRunner = new InnerRunner(copy, i);
			return row.forEachEntry(innerRunner);
		}
	}
	
	/**
	 * Needed for copying.
	 */
	private class InnerRunner implements TIntByteProcedure{
		SparseByteMatrix copy;
		int i;
		InnerRunner(SparseByteMatrix copy, int i){
			this.copy = copy;
			this.i=i;
		}
		@Override
		public boolean execute(int j, byte b) {
			copy.set(i, j, b);
			return true;
		}
	}

	/**
	 * Some test code
	 * @param args -
	 */
	public static void main(String[] args) {
		SparseByteMatrix m = new SparseByteMatrix();
		m.set(3, 3, (byte)1);
		m.set(4, 4, (byte)5);
		System.out.println(m.get(2, 2));
		System.out.println(m.get(3, 3));
		System.out.println(m.get(4, 4));
		System.out.println("COPY");
		SparseByteMatrix n = m.copy();
		n.set(2, 2, (byte)2);
		n.set(3, 3, (byte)0);
		System.out.println("old");
		System.out.println(m.get(2, 2));
		System.out.println(m.get(3, 3));
		System.out.println(m.get(4, 4));
		System.out.println("new");
		System.out.println(n.get(2, 2));
		System.out.println(n.get(3, 3));
		System.out.println(n.get(4, 4));
		
		//System.out.println(m.get(0, 3));
		System.out.println("DONE");
	}

	@Override
	public int getM() {
		return M;
	}

	@Override
	public int getN() {
		return N;
	}

	@Override
	public String getType() {
		return "byte";
	}

	@Override
	public int getNumberOfEntries() {
		return numberOfEntries;
	}
}
