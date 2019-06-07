package com.df.es;

public final class ElasticDataFactory {

	public static ElasticData[] createDemoData(int samples) {
		ElasticData[] data = new ElasticData[samples];
		for (int c = 0; c < samples; c++) {
			data[c] = new ElasticData(String.format("Item-%03d", c), getCategory(c), samples - c);
		}
		return data;
	}
	
	private static String getCategory(int c) {
		if (c % 2 == 0)
			return "GREEN";
		else if (c > 1 && isPrime(c))
			return "RED";
		return "BLUE";
	}
	
	private static boolean isPrime(int n) {
        if (n <= 1) {
            return false;
        }
        if(n == 2){
            return true;
        }
        for (int i = 2; i < n; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }
}
