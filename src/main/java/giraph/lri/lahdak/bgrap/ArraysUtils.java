package giraph.lri.lahdak.bgrap;

public class ArraysUtils {


	public static double divergenceKLU(double[] p) {
		double distance = 0;
		double sum = sum(p);

		double u = ((double) 1) / (p.length);
		for (int i = 0; i < p.length; i++) {
			if (p[i] != 0) {
				double a = p[i] / sum;
				distance += a * Math.log(a / u);
			} else {
				return -1;
			}
		}
		return Math.abs(distance);
	}

	public static double divergenceKLU(long[] p) {
		double distance = 0;
		long sum = sum(p);

		double u = ((double) 1) / (p.length);
		for (int i = 0; i < p.length; i++) {
			if (p[i] != 0) {
				double a = ((double) p[i]) / sum;
				distance += a * Math.log(a / u);
			} else {
				return -1;
			}
		}
		return distance / Math.log(p.length);
	}

	public static double divergenceKLU(int[] p) {
		double distance = 0;
		int sum = sum(p);

		double u = ((double) 1) / (p.length);
		for (int i = 0; i < p.length; i++) {
			if (p[i] != 0) {
				double a = ((double) p[i]) / sum;
				distance += a * Math.log(a / u);
			} else {
				return -1;
			}
		}
		return distance / Math.log(p.length);
	}

	public static double sum(double[] p) {
		double sum = 0;
		for (int i = 0; i < p.length; i++) {
			sum += p[i];
		}
		return sum;
	}

	public static long sum(long[] p) {
		long sum = 0;
		for (int i = 0; i < p.length; i++) {
			sum += p[i];
		}
		return sum;
	}

	public static int sum(int[] p) {
		int sum = 0;
		for (int i = 0; i < p.length; i++) {
			sum += p[i];
		}
		return sum;
	}
}
