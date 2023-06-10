package engine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

import exceptions.DBAppException;

public class DBApp {

	static int maxTuplesPerPage;

	public static void init() throws IOException {
		Properties properties = new Properties();
		FileInputStream fis = new FileInputStream("src/resources/config/DBApp.config");
		properties.load(fis);
		maxTuplesPerPage = Integer.parseInt(properties.getProperty("MaximumRowsCountinTablePage"));
		Node.maxEntriesInOctreeNode = Integer.parseInt(properties.getProperty("MaximumEntriesinOctreeNode"));
	}

	public static boolean doesTableExist(String strTableName) {
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values[0].equals(strTableName))
					return true;
			}
		} catch (Exception e) {
			return false;
		}
		return false;
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		if (!doesTableExist(strTableName)) {
			try {
				new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
			} catch (IOException e) {
				System.out.println("IOException occurred");
			}
		} else
			throw new DBAppException("Can not create a table that already exists");
	}

	public static void validateEntry(String strTableName, Hashtable<String, Object> htblColNameValue, boolean insert)
			throws FileNotFoundException, IOException, DBAppException {
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;

			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					Object givenValue = htblColNameValue.get(values[1].substring(1));
					if (givenValue == null)
						continue;

					if (givenValue == "null")
						continue;

					if (values[2].substring(1).equals("java.lang.String")) {
						if (!(givenValue instanceof String))
							throw new DBAppException("Invalid data type");

						if (((String) givenValue).compareToIgnoreCase(values[6].substring(1)) < 0)
							throw new DBAppException("Can not insert value less than min accepted value");

						if (((String) givenValue).compareToIgnoreCase(values[7].substring(1)) > 0)
							throw new DBAppException("Can not insert value greater than max accepted value");
					}

					if (values[2].substring(1).equals("java.lang.Integer")) {
						if (!(givenValue instanceof Integer))
							throw new DBAppException("Invalid data type");

						if ((int) givenValue < Integer.parseInt(values[6].substring(1)))
							throw new DBAppException("Can not insert value less than min accepted value");

						if ((int) givenValue > Integer.parseInt(values[7].substring(1)))
							throw new DBAppException("Can not insert value greater than max accepted value");
					}

					if (values[2].substring(1).equals("java.lang.Double")) {
						if (!(givenValue instanceof Double))
							throw new DBAppException("Invalid data type");

						if ((double) givenValue < Double.parseDouble(values[6].substring(1)))
							throw new DBAppException("Can not insert value less than min accepted value");

						if ((double) givenValue > Double.parseDouble(values[7].substring(1)))
							throw new DBAppException("Can not insert value greater than max accepted value");
					}

					if (values[2].substring(1).equals("java.util.Date")) {
						Date inputDate = (Date) givenValue;
						String[] d = values[6].substring(1).split("-");
						String[] d1 = values[7].substring(1).split("-");
						int y = Integer.parseInt(d[0]);
						int m = Integer.parseInt(d[1]);
						int day = Integer.parseInt(d[2]);
						int y2 = Integer.parseInt(d1[0]);
						int m2 = Integer.parseInt(d1[1]);
						int day2 = Integer.parseInt(d1[2]);
						Date minDate = new Date(y - 1900, m - 1, day);
						Date maxDate = new Date(y2 - 1900, m2 - 1, day2);

						if ((inputDate).compareTo(minDate) < 0)
							throw new DBAppException("Can not insert value less than min accepted value");

						if ((inputDate).compareTo(maxDate) > 0)
							throw new DBAppException("Can not insert value greater than max accepted value");
					}

					if (givenValue != null && values[3].substring(1).equals("True") && insert == true) {
						validateDuplicates(strTableName, htblColNameValue.get(values[1].substring(1)));
					}
				}
			}
		}
	}

	public static void validateDuplicates(String strTableName, Object clusteringKey)
			throws FileNotFoundException, IOException, DBAppException {
		Table table = Table.readFromFile(strTableName);

		if (table.numOfPages == 0)
			return;

		Page page;

		for (int i = 1; i <= table.numOfPages; i++) {
			page = Page.readFromFile(strTableName, i);
			for (int j = 0; j < page.rows.size(); j++) {
				if (page.rows.get(j).get(getClusteringKeyName(strTableName)).equals(clusteringKey))
					throw new DBAppException("Primary key already exists. Can not have duplicate primary keys");
			}
		}
	}

	public static Object getClusteringKeyName(String strTableName) throws FileNotFoundException, IOException {
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values[0].equals(strTableName)) {
					if (values[3].substring(1).equals("True"))
						return values[1].substring(1);
				}
			}
		}

		return "";
	}

	public static String getClusteringKeyType(String strTableName) throws FileNotFoundException, IOException {
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values[0].equals(strTableName)) {
					if (values[3].substring(1).equals("True"))
						return values[2].substring(1);
				}
			}
		}

		return "";
	}

	public static boolean check(String strTableName, Hashtable<String, Object> htblColNameValue, Page page, boolean min,
			int check) throws FileNotFoundException, IOException {
		String clusteringKeyType = getClusteringKeyType(strTableName);

		if (clusteringKeyType.equals("java.lang.Integer")) {
			int newValue = (int) htblColNameValue.get(getClusteringKeyName(strTableName));

			if ((newValue >= (Integer) page.minValue) && (newValue <= (Integer) page.maxValue) && check == 1
					&& min == false)
				return true;

			if ((newValue < (Integer) page.minValue) && min && check == 0)
				return true;

			if (newValue > (Integer) page.maxValue && !min && check == 0)
				return true;
		}

		if (clusteringKeyType.equals("java.lang.Double")) {
			double newValue = (double) htblColNameValue.get(getClusteringKeyName(strTableName));

			if ((newValue >= (Double) page.minValue) && (newValue <= (Double) page.maxValue) && check == 1
					&& min == false)
				return true;

			if (newValue < (Double) page.minValue && min && check == 0)
				return true;

			if (newValue > (Double) page.maxValue && !min && check == 0)
				return true;
		}

		if (clusteringKeyType.equals("java.lang.String")) {
			String newValue = (String) htblColNameValue.get(getClusteringKeyName(strTableName));

			if (newValue.compareToIgnoreCase((String) page.minValue) >= 0
					&& (newValue.compareToIgnoreCase((String) page.maxValue)) <= 0 && check == 1 && min == false)
				return true;

			if (newValue.compareToIgnoreCase((String) page.minValue) < 0 && min && check == 0)
				return true;

			if (newValue.compareToIgnoreCase((String) page.maxValue) > 0 && !min && check == 0)
				return true;
		}

		if (clusteringKeyType.equals("java.util.Date")) {
			Date newValue = (Date) htblColNameValue.get(getClusteringKeyName(strTableName));

			if (newValue.compareTo((Date) page.minValue) >= 0 && (newValue.compareTo((Date) page.maxValue)) <= 0
					&& check == 1 && min == false)
				return true;

			if (newValue.compareTo((Date) page.minValue) < 0 && min && check == 0)
				return true;

			if (newValue.compareTo((Date) page.maxValue) > 0 && !min && check == 0)
				return true;
		}

		return false;
	}

	public static int binarySearch(String strTableName, Page page, Object newValue)
			throws FileNotFoundException, IOException {
		String clusteringKeyType = getClusteringKeyType(strTableName);

		int left = 0;
		int right = page.rows.size() - 1;

		if (clusteringKeyType.equals("java.lang.Integer")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if ((int) newValue > (int) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
					left = middle + 1;
				else
					right = middle - 1;
			}
		}

		if (clusteringKeyType.equals("java.lang.Double")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if ((double) newValue > (double) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
					left = middle + 1;
				else
					right = middle - 1;
			}
		}

		if (clusteringKeyType.equals("java.lang.String")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if (((String) newValue).compareToIgnoreCase(
						(String) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
					left = middle + 1;
				else
					right = middle - 1;
			}
		}

		if (clusteringKeyType.equals("java.util.Date")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if (((Date) newValue)
						.compareTo((Date) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
					left = middle + 1;
				else
					right = middle - 1;
			}
		}

		return left;
	}

	public static int binarySearch2(String strTableName, Page page, Object value)
			throws FileNotFoundException, IOException {
		String clusteringKeyType = getClusteringKeyType(strTableName);

		int left = 0;
		int right = page.rows.size() - 1;

		if (clusteringKeyType.equals("java.lang.Integer")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if (page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;

				else {
					if ((int) value > (int) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}

		if (clusteringKeyType.equals("java.lang.Double")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if (page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;

				else {
					if ((double) value > (double) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}

		if (clusteringKeyType.equals("java.lang.String")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if (page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;

				else {
					if (((String) value).compareToIgnoreCase(
							(String) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}

		if (clusteringKeyType.equals("java.util.Date")) {
			while (left <= right) {
				int middle = (left + right) / 2;

				if (page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;

				else {
					if (((Date) value)
							.compareTo((Date) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}

		return -1;
	}

	public void createNewPage(Table table, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		Page page = new Page();
		table.numOfPages++;

		page.rows.add(htblColNameValue);
		page.minValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
		page.maxValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
		Page.writeToFile(page, table.strTableName, table.numOfPages);

		if (table.numOfIndices > 0) {
			Octree octree;
			HashSet<String> indices = getIndicesOnTable(table.strTableName);

			for (String indexName : indices) {
				octree = Octree.readFromFile(indexName);

				octree.delete(page.rows.get(0).get(getClusteringKeyName(table.strTableName)), page.rows.get(0));
				octree.insert(page.rows.get(0).get(getClusteringKeyName(table.strTableName)), table.numOfPages,
						page.rows.get(0));

				Octree.writeToFile(octree);
			}

//			insertIntoIndex(table.strTableName, page.rows.get(0), table.numOfPages);
		}

		Table.writeToFile(table);
	}

	public void shiftDown(Table table, int idx, Hashtable<String, Object> shiftedRow)
			throws FileNotFoundException, IOException {
		if (idx > table.numOfPages) {
			createNewPage(table, shiftedRow);
			return;
		}

		Page tmpPage = Page.readFromFile(table.strTableName, idx);
		if (idx == table.numOfPages && tmpPage.rows.size() == maxTuplesPerPage) {
			tmpPage.rows.add(0, shiftedRow);
			tmpPage.minValue = shiftedRow.get(getClusteringKeyName(table.strTableName));
			shiftedRow = tmpPage.rows.remove(tmpPage.rows.size() - 1);
			tmpPage.maxValue = tmpPage.rows.get((tmpPage.rows.size() - 1))
					.get(getClusteringKeyName(table.strTableName));

			Page.writeToFile(tmpPage, table.strTableName, idx);
			createNewPage(table, shiftedRow);
			return;
		}

		Page page;
		for (int i = idx; i <= table.numOfPages; i++) {
			page = Page.readFromFile(table.strTableName, i);
			page.rows.add(0, shiftedRow);
			page.minValue = shiftedRow.get(getClusteringKeyName(table.strTableName));
			Page.writeToFile(page, table.strTableName, i);

			Octree octree;
			HashSet<String> indices = getIndicesOnTable(table.strTableName);

			for (String indexName : indices) {
				octree = Octree.readFromFile(indexName);

				octree.delete(shiftedRow.get(getClusteringKeyName(table.strTableName)), shiftedRow);
				octree.insert(shiftedRow.get(getClusteringKeyName(table.strTableName)), i, shiftedRow);

				Octree.writeToFile(octree);
			}

			if (page.rows.size() <= maxTuplesPerPage)
				break;

			shiftedRow = page.rows.remove(page.rows.size() - 1);
			page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(table.strTableName));
			Page.writeToFile(page, table.strTableName, i);

			indices = getIndicesOnTable(table.strTableName);

			for (String indexName : indices) {
				octree = Octree.readFromFile(indexName);

				octree.delete(shiftedRow.get(getClusteringKeyName(table.strTableName)), shiftedRow);
				octree.insert(shiftedRow.get(getClusteringKeyName(table.strTableName)), i, shiftedRow);

				Octree.writeToFile(octree);
			}

			if (i == table.numOfPages) {
				createNewPage(table, shiftedRow);
				break;
			}
		}
	}

	public static HashSet<String> getIndicesOnTable(String strTableName) {
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;
			HashSet<String> indices = new HashSet<String>();
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values[0].equals(strTableName)) {
					if (!values[4].substring(1).equals("null"))
						indices.add(strTableName + "_" + values[4].substring(1));
				}
			}

			return indices;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public void insertIntoIndex(String strTableName, Hashtable<String, Object> htblColNameValue, int i) {
		HashSet<String> indices = getIndicesOnTable(strTableName);
		Octree octree;

		for (String indexName : indices) {
			octree = Octree.readFromFile(indexName);

			try {
				octree.insert(htblColNameValue.get(getClusteringKeyName(strTableName)), i, htblColNameValue);
			} catch (IOException e) {
				e.printStackTrace();
			}

			Octree.writeToFile(octree);
		}
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			validateEntry(strTableName, htblColNameValue, true);

			if (htblColNameValue.get(getClusteringKeyName(strTableName)) == null)
				throw new DBAppException("Can not insert a record with no primary key");

			Table table = Table.readFromFile(strTableName);

			if (htblColNameValue.size() < table.htblColNameType.size()) {
				for (String colName : table.htblColNameType.keySet()) {
					if (!(htblColNameValue.containsKey(colName)))
						htblColNameValue.put(colName, "null");
				}
			}

			for (String colName : htblColNameValue.keySet()) {
				if (table.htblColNameType.get(colName) == null)
					throw new DBAppException("Can not insert column " + colName + " since it doesn't exist.");
			}

			if (table.numOfPages == 0) {
				createNewPage(table, htblColNameValue);
				return;
			}

			Page page;
			boolean lessThanMin;
			boolean greaterThanMax;
			boolean inBetween;

			int size = table.numOfPages;

			for (int i = 1; i <= size; i++) {
				page = Page.readFromFile(strTableName, i);
				lessThanMin = check(strTableName, htblColNameValue, page, true, 0);
				greaterThanMax = check(strTableName, htblColNameValue, page, false, 0);
				inBetween = check(strTableName, htblColNameValue, page, false, 1);

				if (inBetween) {
					int pos = binarySearch(strTableName, page,
							htblColNameValue.get(getClusteringKeyName(strTableName)));
					page.rows.add(pos, htblColNameValue);

					if (page.rows.size() > maxTuplesPerPage) {
						Hashtable<String, Object> shiftedRow = page.rows.remove(page.rows.size() - 1);
						page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(strTableName));

						shiftDown(table, i + 1, shiftedRow);
					}
					Page.writeToFile(page, strTableName, i);

					if (table.numOfIndices > 0)
						insertIntoIndex(strTableName, page.rows.get(pos), i);

					break;
				}

				if (lessThanMin) {
					page.rows.add(0, htblColNameValue);
					page.minValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));

					if (page.rows.size() > maxTuplesPerPage) {
						Hashtable<String, Object> shiftedRow = page.rows.remove(page.rows.size() - 1);
						page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(strTableName));

						shiftDown(table, i + 1, shiftedRow);
					}
					Page.writeToFile(page, strTableName, i);

					if (table.numOfIndices > 0)
						insertIntoIndex(strTableName, page.rows.get(0), i);

					break;
				}

				if (greaterThanMax) {
					if (page.rows.size() >= maxTuplesPerPage) {
						if (i == table.numOfPages) {
							createNewPage(table, htblColNameValue);
							break;
						}
					} else {
						page.rows.add(page.rows.size(), htblColNameValue);
						page.maxValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
						Page.writeToFile(page, strTableName, i);

						if (table.numOfIndices > 0)
							insertIntoIndex(strTableName, page.rows.get(page.rows.size() - 1), i);

						break;
					}
				}
			}

			Table.writeToFile(table);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			validateEntry(strTableName, htblColNameValue, true);

			if (htblColNameValue.get(getClusteringKeyName(strTableName)) != null)
				throw new DBAppException("Can not update primary key");

			Table table = Table.readFromFile(strTableName);

			for (String colName : htblColNameValue.keySet()) {
				if (table.htblColNameType.get(colName) == null)
					throw new DBAppException("Can not update column " + colName + " since it doesn't exist.");
			}

			Page page;

			Hashtable<String, Object> clusteringKey = new Hashtable<String, Object>();
			String clusteringKeyType = getClusteringKeyType(strTableName);

			if (clusteringKeyType.equals("java.lang.Integer"))
				clusteringKey.put((String) getClusteringKeyName(strTableName), Integer.parseInt(strClusteringKeyValue));

			if (clusteringKeyType.equals("java.lang.Double"))
				clusteringKey.put((String) getClusteringKeyName(strTableName),
						Double.parseDouble(strClusteringKeyValue));

			if (clusteringKeyType.equals("java.lang.String"))
				clusteringKey.put((String) getClusteringKeyName(strTableName), (String) strClusteringKeyValue);

			if (clusteringKeyType.equals("java.util.Date")) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");
				try {
					clusteringKey.put((String) getClusteringKeyName(strTableName),
							dateFormat.parse(strClusteringKeyValue));
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}

			boolean inBetween;
			boolean flag = false;
			int size = table.numOfPages;

			for (int i = 1; i <= size; i++) {
				page = Page.readFromFile(strTableName, i);
				inBetween = check(strTableName, clusteringKey, page, false, 1);
				flag = false;
				if (inBetween) {
					int pos = binarySearch2(strTableName, page, clusteringKey.get(getClusteringKeyName(strTableName)));
					if (pos == -1)
						throw new DBAppException("Can not update record that does not exist");

					Hashtable<String, Object> oldValue = page.rows.get(pos);
					Octree octree;
					HashSet<String> indices = getIndicesOnTable(strTableName);

					for (String indexName : indices) {
						octree = Octree.readFromFile(indexName);

						octree.update(page.rows.get(pos).get(getClusteringKeyName(strTableName)), oldValue,
								htblColNameValue);

						Octree.writeToFile(octree);
					}

					for (String colName : htblColNameValue.keySet())
						page.rows.get(pos).replace(colName, htblColNameValue.get(colName));

					Page.writeToFile(page, strTableName, i);
					flag = true;
					break;
				}
			}

			if (!flag)
				throw new DBAppException("Can not update record that does not exist");

			Table.writeToFile(table);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean satisfies(Hashtable<String, Object> row, Hashtable<String, Object> htblColNameValue) {
		for (String colName : htblColNameValue.keySet()) {
			if (!(htblColNameValue.get(colName).equals(row.get(colName))))
				return false;
		}

		return true;
	}

	public static void shiftUp(Table table) throws FileNotFoundException, IOException {
		Page page;
		Page tmpPage;
		int c;

		for (int i = 1; i <= table.numOfPages; i++) {
			page = Page.readFromFile(table.strTableName, i);
			c = i + 1;

			while (page.rows.size() < maxTuplesPerPage) {
				if (i >= table.numOfPages)
					break;

				if (c > table.numOfPages)
					break;

				tmpPage = Page.readFromFile(table.strTableName, c);

				for (int j = 1; j <= tmpPage.rows.size(); j++) {
					Octree octree;
					HashSet<String> indices = getIndicesOnTable(table.strTableName);

					for (String indexName : indices) {
						octree = Octree.readFromFile(indexName);

						octree.delete(tmpPage.rows.get(0).get(getClusteringKeyName(table.strTableName)),
								tmpPage.rows.get(0));
						octree.insert(tmpPage.rows.get(0).get(getClusteringKeyName(table.strTableName)), c - 1,
								tmpPage.rows.get(0));

						Octree.writeToFile(octree);
					}

					page.rows.add(page.rows.size(), tmpPage.rows.remove(0));

					if (page.rows.size() == maxTuplesPerPage)
						break;
				}

				Page.writeToFile(tmpPage, table.strTableName, c);
				c++;
			}

			if (!page.rows.isEmpty()) {
				page.minValue = page.rows.get(0).get(getClusteringKeyName(table.strTableName));
				page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(table.strTableName));
			}
			Page.writeToFile(page, table.strTableName, i);
		}
	}

	public static void cleanUp(Table table) {
		Page page;
		for (int i = table.numOfPages; i > 0; i--) {
			page = Page.readFromFile(table.strTableName, i);

			if (page.rows.isEmpty()) {
				String pageName = table.strTableName + "_" + i;
				File file = new File("src/resources/docs/pages/" + pageName + ".ser");
				file.delete();
				table.numOfPages--;
			}
		}
	}

	public boolean indexInvalid(Table table, Hashtable<String, Object> htblColNameValue) {
		if (table.numOfIndices == 0)
			return true;

		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			double numOfIndexedColumns = 0;
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values.length == 8) {
					if (values[0].equals(table.strTableName) && htblColNameValue.containsKey(values[1].substring(1))
							&& !values[4].substring(1).equals("null")) {
						numOfIndexedColumns++;
					}
				}
			}

			if (numOfIndexedColumns / table.numOfIndices != 3) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return false;
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			if (htblColNameValue.isEmpty())
				throw new DBAppException("Please input something to delete");

			validateEntry(strTableName, htblColNameValue, false);

			boolean clusteringKeyCheck = false;
			Hashtable<String, Object> clusteringKey = new Hashtable<String, Object>();

			if (htblColNameValue.get(getClusteringKeyName(strTableName)) != null) {
				clusteringKey.put((String) getClusteringKeyName(strTableName),
						htblColNameValue.get(getClusteringKeyName(strTableName)));
				clusteringKeyCheck = true;
			}

			Table table = Table.readFromFile(strTableName);
			Page page;
			boolean inBetween;

			if (indexInvalid(table, htblColNameValue)) {
				for (int i = 1; i <= table.numOfPages; i++) {
					page = Page.readFromFile(strTableName, i);

					if (clusteringKeyCheck) {
						inBetween = check(strTableName, clusteringKey, page, false, 1);

						if (inBetween) {
							int pos = binarySearch2(strTableName, page,
									clusteringKey.get(getClusteringKeyName(strTableName)));
							if (pos == -1)
								throw new DBAppException("Can not delete record that does not exist");

							if (satisfies(page.rows.get(pos), htblColNameValue)) {
								Octree octree;
								HashSet<String> indices = getIndicesOnTable(strTableName);

								for (String indexName : indices) {
									octree = Octree.readFromFile(indexName);

									octree.delete(htblColNameValue.get(getClusteringKeyName(strTableName)),
											page.rows.get(pos));

									Octree.writeToFile(octree);
								}

								page.rows.remove(pos);
							}

							if (!page.rows.isEmpty()) {
								page.minValue = page.rows.get(0).get(getClusteringKeyName(strTableName));
								page.maxValue = page.rows.get((page.rows.size() - 1))
										.get(getClusteringKeyName(strTableName));
							}
							Page.writeToFile(page, strTableName, i);
							break;
						}
					}

					else {
						for (int j = 0; j < page.rows.size(); j++) {
							if (satisfies(page.rows.get(j), htblColNameValue)) {
								Octree octree;
								HashSet<String> indices = getIndicesOnTable(strTableName);

								for (String indexName : indices) {
									octree = Octree.readFromFile(indexName);

									octree.delete(page.rows.get(j).get(getClusteringKeyName(strTableName)),
											page.rows.get(j));

									Octree.writeToFile(octree);
								}

								page.rows.remove(j--);
							}

							if (!page.rows.isEmpty()) {
								page.minValue = page.rows.get(0).get(getClusteringKeyName(strTableName));
								page.maxValue = page.rows.get((page.rows.size() - 1))
										.get(getClusteringKeyName(strTableName));
							}
						}

						Page.writeToFile(page, strTableName, i);
					}
				}
			} else {
				HashSet<Object[]> primaryKeyAndPageNumber = new HashSet<Object[]>();
				HashSet<Object[]> tmp = new HashSet<Object[]>();
				HashSet<String> indices = getIndicesOnTable(strTableName);
				Octree octree;

				for (String indexName : indices) {
					octree = Octree.readFromFile(indexName);

					tmp = octree.getRowsToBeDeleted(htblColNameValue);

					for (Object[] x : tmp) {
						primaryKeyAndPageNumber.add(x);
					}

					Octree.writeToFile(octree);
				}

				Page tmpPage;
				for (Object[] i : primaryKeyAndPageNumber) {
					tmpPage = Page.readFromFile(strTableName, (int) i[1]);
					int pos = binarySearch2(strTableName, tmpPage, (int) i[0]);
					for (String indexName : indices) {
						octree = Octree.readFromFile(indexName);

						if (pos != -1)
							octree.delete(i[0], tmpPage.rows.get(pos));

						Octree.writeToFile(octree);
					}

					if (pos != -1)
						tmpPage.rows.remove(pos);

					Page.writeToFile(tmpPage, strTableName, (int) i[1]);
				}
			}

			shiftUp(table);
			cleanUp(table);

			Table.writeToFile(table);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		HashSet<String> columnNames = new HashSet<String>();

		for (int i = 0; i < strarrColName.length; i++)
			columnNames.add(strarrColName[i]);

		if (columnNames.size() != 3)
			throw new DBAppException("Index must be created on 3 different columns");

		Table table = Table.readFromFile(strTableName);

		for (String colName : columnNames) {
			if (table.htblColNameType.get(colName) == null)
				throw new DBAppException("Index cannot be created on column that does not exist.");
		}

		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;
			Pair[] pairs = new Pair[3];
			int c = 0;

			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					if (columnNames.contains(values[1].substring(1))) {
						if (!values[4].substring(1).equals("null")) {
							throw new DBAppException("Index already exists");
						}

						if (values[2].substring(1).equals("java.lang.Integer"))
							pairs[c++] = new Pair(Integer.parseInt(values[6].substring(1)),
									Integer.parseInt(values[7].substring(1)));

						if (values[2].substring(1).equals("java.lang.Double"))
							pairs[c++] = new Pair(Double.parseDouble(values[6].substring(1)),
									Double.parseDouble(values[7].substring(1)));

						if (values[2].substring(1).equals("java.lang.String"))
							pairs[c++] = new Pair(values[6].substring(1), values[7].substring(1));

						if (values[2].substring(1).equals("java.util.Date")) {
							SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");
							Date date1 = dateFormat.parse(values[6].substring(1));
							Date date2 = dateFormat.parse(values[7].substring(1));

							pairs[c++] = new Pair(date1, date2);
						}
					}
				}
			}
			Octree octree = new Octree(table.strTableName, pairs[0], pairs[1], pairs[2], strarrColName);

			table.numOfIndices++;

			if (table.numOfPages > 0) {
				Page page;
				for (int i = 1; i <= table.numOfPages; i++) {
					page = Page.readFromFile(table.strTableName, i);

					for (int j = 0; j < page.rows.size(); j++) {
						octree.insert(page.rows.get(j).get(getClusteringKeyName(strTableName)), i, page.rows.get(j));
					}
				}
			}

			Octree.writeToFile(octree);
			Table.writeToFile(table);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		ArrayList<Hashtable<String, Object>> resultSet = new ArrayList<Hashtable<String, Object>>();
		ArrayList<Hashtable<String, Object>> tmp1 = new ArrayList<Hashtable<String, Object>>();
		ArrayList<String> tmp2 = new ArrayList<String>();
		ArrayList<Object> tmp3 = new ArrayList<Object>();
		ArrayList<Integer> indexed = new ArrayList<Integer>();
		ArrayList<String> IndexCheck = new ArrayList<String>();
		Hashtable<String, Object> s = new Hashtable<String, Object>();
		Hashtable<String, String> op = new Hashtable<String, String>();
		Table table = null;
		Page page;
		int pos = -1;
		int PageNum = 0;
		if (arrSQLTerms.length >= 1)
			table = Table.readFromFile(arrSQLTerms[0].strTableName);
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if ((i + 1 < arrSQLTerms.length) && (i + 2 < arrSQLTerms.length)) {
				if (strarrOperators[i].equals("AND") && strarrOperators[i + 1].equals("AND")) {
					IndexCheck.add(arrSQLTerms[i].strTableName);
					IndexCheck.add(arrSQLTerms[i].strColumnName);
					IndexCheck.add(arrSQLTerms[i + 1].strColumnName);
					IndexCheck.add(arrSQLTerms[i + 2].strColumnName);
					if (CheckIndex(IndexCheck)) {

						indexed.add(i);
						indexed.add(i + 1);
						indexed.add(i + 2);
						i = i + 2;
					}
					IndexCheck.clear();
				}
			}
		}
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (indexed.contains(i)) {
				s.put(arrSQLTerms[i].strColumnName, arrSQLTerms[i].objValue);
				s.put(arrSQLTerms[i + 1].strColumnName, arrSQLTerms[i + 1].objValue);
				s.put(arrSQLTerms[i + 2].strColumnName, arrSQLTerms[i + 2].objValue);
				op.put(arrSQLTerms[i].strColumnName, arrSQLTerms[i].strOperator);
				op.put(arrSQLTerms[i + 1].strColumnName, arrSQLTerms[i + 1].strOperator);
				op.put(arrSQLTerms[i + 2].strColumnName, arrSQLTerms[i + 2].strOperator);
				tmp2.add(arrSQLTerms[i].strColumnName);
				tmp2.add(arrSQLTerms[i + 1].strColumnName);
				tmp2.add(arrSQLTerms[i + 2].strColumnName);
				Octree index = getOctree(arrSQLTerms, i, tmp3);

				ArrayList<Node> d = new ArrayList<Node>();

				d = index.getTuples(tmp3, d);
				resultSet = NodeCheck(d, s, op, tmp2, arrSQLTerms[i].strTableName);

				i = i + 2;
			} else {

				if (i == 0) {
					resultSet = getTuples(arrSQLTerms[i].strTableName, arrSQLTerms[i].strColumnName,
							arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue);

				} else {
					String StarOp = strarrOperators[i - 1];
					if (StarOp.equals("OR")) {
						tmp1 = getTuples(arrSQLTerms[i].strTableName, arrSQLTerms[i].strColumnName,
								arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue);
						resultSet = OR(resultSet, tmp1);
					} else if (StarOp.equals("AND")) {
						tmp1 = getTuples(arrSQLTerms[i].strTableName, arrSQLTerms[i].strColumnName,
								arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue);
						resultSet = AND(resultSet, tmp1);
					} else {
						tmp1 = getTuples(arrSQLTerms[i].strTableName, arrSQLTerms[i].strColumnName,
								arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue);
						resultSet = XOR(resultSet, tmp1);
					}
				}

			}

		}
		Iterator<Hashtable<String, Object>> it = resultSet.iterator();

		return it;

	}

	public ArrayList<Hashtable<String, Object>> NodeCheck(ArrayList<Node> node, Hashtable<String, Object> r,
			Hashtable<String, String> d, ArrayList<String> colnames, String strTableName) {
		ArrayList<Hashtable<String, Object>> resultSet = new ArrayList<Hashtable<String, Object>>();
		for (int i = 0; i < node.size(); i++) {
			for (int j = 0; j < node.get(i).data.size(); j++) {

				Hashtable<String, Object> tmp = (Hashtable<String, Object>) node.get(i).data.get(j)[2];
				String x = colnames.get(0);
				String y = colnames.get(1);
				String z = colnames.get(2);

				if (satsfies(tmp.get(x), r.get(x), d.get(x)) && satsfies(tmp.get(y), r.get(y), d.get(y))
						&& satsfies(tmp.get(z), r.get(z), d.get(z))) {
					resultSet.add(getRow(strTableName, (int) node.get(i).data.get(j)[1], node.get(i).data.get(j)[0]));
				}

			}
		}
		return resultSet;
	}

	public static Hashtable<String, Object> getRow(String strTableName, int pageNum, Object key) {
		Page page = Page.readFromFile(strTableName, pageNum);
		try {
			return page.rows.get(binarySearch2(strTableName, page, key));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public boolean satsfies(Object tuple, Object objValue, String strOperator) {
		if (strOperator.equals("=") && Equal(tuple, objValue)) {
			return true;
		} else if (strOperator.equals("!=") && NotEqual(objValue, tuple)) {
			return true;
		} else if (strOperator.equals("<") && LessThan(tuple, objValue)) {
			return true;
		} else if (strOperator.equals("<=") && LessThanOrEqual(tuple, objValue)) {
			return true;
		} else if (strOperator.equals(">") && GreaterThan(tuple, objValue)) {

			return true;
		} else if (strOperator.equals(">=") && GreaterThanOrEqual(tuple, objValue)) {
			return true;
		}
		return false;
	}

	public Octree getOctree(SQLTerm[] arrSQLTerms, int s, ArrayList<Object> r) {
		Table table = Table.readFromFile(arrSQLTerms[s].strTableName);
		String index = "";
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			double numOfIndexedColumns = 0;
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values.length == 8) {
					if (values[0].equals(table.strTableName) && !values[4].substring(1).equals("null"))

					{
						String[] i = values[4].substring(1).split("_");

						if (contains(i, arrSQLTerms[s].strColumnName) && contains(i, arrSQLTerms[s + 1].strColumnName)
								&& contains(i, arrSQLTerms[s + 2].strColumnName)) {

							if (values[1].substring(1).equals(arrSQLTerms[s].strColumnName)) {
								r.add(arrSQLTerms[s].objValue);
							}
							if (values[1].substring(1).equals(arrSQLTerms[s + 1].strColumnName)) {

								r.add(arrSQLTerms[s + 1].objValue);
							}
							if (values[1].substring(1).equals(arrSQLTerms[s + 2].strColumnName)) {

								r.add(arrSQLTerms[s + 2].objValue);
							}
							index = values[4].substring(1);

						}

					}
				}
			}

		}

		catch (Exception e) {
			e.printStackTrace();

		}

		Octree y = Octree.readFromFile(arrSQLTerms[0].strTableName + "_" + index);

		return y;

	}

	public boolean CheckIndex(ArrayList<String> x) {
		Table table = Table.readFromFile(x.get(0));
		if (table.numOfIndices == 0)
			return false;
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			double numOfIndexedColumns = 0;
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values.length == 8) {
					if (values[0].equals(table.strTableName) && !values[4].substring(1).equals("null"))

					{
						String[] i = values[4].substring(1).split("_");

						if (contains(i, x.get(1)) && contains(i, x.get(2)) && contains(i, x.get(3))) {
							return true;
						}

					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return false;

	}

	public boolean contains(String[] x, String y) {
		for (int i = 0; i < x.length; i++) {
			if (x[i].equals(y))
				return true;
		}
		return false;
	}

	public ArrayList<Hashtable<String, Object>> getTuples(String strTableName, String strColumnName, String strOperator,
			Object objValue) {
		ArrayList<Hashtable<String, Object>> resultSet = new ArrayList<Hashtable<String, Object>>();
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put(strColumnName, objValue);
		Table table = Table.readFromFile(strTableName);
		Page page;

		for (int i = 1; i <= table.numOfPages; i++) {
			page = Page.readFromFile(strTableName, i);
			for (int j = 0; j < page.rows.size(); j++) {
				Object tuple = page.rows.get(j).get(strColumnName);

				if (strOperator.equals("=") && Equal(objValue, tuple)) {

					resultSet.add(page.rows.get(j));
				} else if (strOperator.equals("!=") && NotEqual(objValue, tuple)) {
					resultSet.add(page.rows.get(j));
				} else if (strOperator.equals("<") && LessThan(tuple, objValue)) {
					resultSet.add(page.rows.get(j));
				} else if (strOperator.equals("<=") && LessThanOrEqual(tuple, objValue)) {
					resultSet.add(page.rows.get(j));
				} else if (strOperator.equals(">") && GreaterThan(tuple, objValue)) {
					resultSet.add(page.rows.get(j));
				} else if (strOperator.equals(">=") && GreaterThanOrEqual(tuple, objValue)) {
					resultSet.add(page.rows.get(j));
				}
			}

		}

		return resultSet;
	}

	public ArrayList<Hashtable<String, Object>> OR(ArrayList<Hashtable<String, Object>> x,
			ArrayList<Hashtable<String, Object>> y) {

		for (int i = 0; i < y.size(); i++) {
			if (!(x.contains(y.get(i))))
				x.add(y.get(i));

		}
		return x;
	}

	public ArrayList<Hashtable<String, Object>> AND(ArrayList<Hashtable<String, Object>> x,
			ArrayList<Hashtable<String, Object>> y) {

		ArrayList<Hashtable<String, Object>> r = new ArrayList<Hashtable<String, Object>>();
		for (int i = 0; i < y.size(); i++) {
			if ((x.contains(y.get(i))))
				r.add(y.get(i));

		}
		return r;
	}

	public ArrayList<Hashtable<String, Object>> XOR(ArrayList<Hashtable<String, Object>> x,
			ArrayList<Hashtable<String, Object>> y) {

		ArrayList<Hashtable<String, Object>> r = new ArrayList<Hashtable<String, Object>>();
		for (int i = 0; i < x.size(); i++) {
			if (!(y.contains(x.get(i))))
				r.add(x.get(i));

		}
		for (int i = 0; i < y.size(); i++) {
			if (!(x.contains(y.get(i))))
				r.add(y.get(i));

		}
		return r;
	}

	public boolean LessThan(Object x, Object y) {
		if (x instanceof Integer) {
			if ((int) x < (int) y)
				return true;
		} else if (x instanceof Double) {
			if ((Double) x < (Double) y)
				return true;
		} else if (x instanceof String) {
			if (((String) x).compareToIgnoreCase((String) y) < 0)
				return true;
		} else {
			if (((Date) x).compareTo((Date) y) < 0)
				return true;
		}
		return false;
	}

	public boolean LessThanOrEqual(Object x, Object y) {
		if (x instanceof Integer) {
			if ((int) x <= (int) y)
				return true;
		} else if (x instanceof Double) {
			if ((Double) x <= (Double) y)
				return true;
		} else if (x instanceof String) {
			if (((String) x).compareToIgnoreCase((String) y) <= 0)
				return true;
		} else {
			if (((Date) x).compareTo((Date) y) <= 0)
				return true;
		}
		return false;
	}

	public boolean GreaterThan(Object x, Object y) {
		if (x instanceof Integer) {
			if ((int) x > (int) y)
				return true;
		} else if (x instanceof Double) {
			if ((Double) x > (Double) y)
				return true;
		} else if (x instanceof String) {
			if (((String) x).compareToIgnoreCase((String) y) > 0)
				return true;
		} else {
			if (((Date) x).compareTo((Date) y) > 0)
				return true;
		}
		return false;
	}

	public boolean GreaterThanOrEqual(Object x, Object y) {
		if (x instanceof Integer) {
			if ((int) x >= (int) y)
				return true;
		} else if (x instanceof Double) {
			if ((Double) x >= (Double) y)
				return true;
		} else if (x instanceof String) {
			if (((String) x).compareToIgnoreCase((String) y) >= 0)
				return true;
		} else {
			if (((Date) x).compareTo((Date) y) >= 0)
				return true;
		}
		return false;
	}

	public boolean Equal(Object x, Object y) {
		if (x instanceof Integer) {
			if ((int) x == (int) y)
				return true;
		} else if (x instanceof Double) {
			Double Y1 = (Double) y;
			Double X1 = (Double) y;
			if (X1.equals(Y1))
				return true;
		} else if (x instanceof String) {
			if (((String) x).compareToIgnoreCase((String) y) == 0)
				return true;
		} else {
			if (((Date) x).compareTo((Date) y) == 0)
				return true;
		}
		return false;
	}

	public boolean NotEqual(Object x, Object y) {
		if (x instanceof Integer) {
			if ((int) x != (int) y)
				return true;
		} else if (x instanceof Double) {
			if ((Double) x != (Double) y)
				return true;
		} else if (x instanceof String) {
			if (((String) x).compareToIgnoreCase((String) y) != 0)
				return true;
		} else {
			if (((Date) x).compareTo((Date) y) != 0)
				return true;
		}
		return false;
	}

	private static void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
		BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
		String record;
		Hashtable<String, Object> row = new Hashtable<>();
		int c = limit;
		if (limit == -1) {
			c = 1;
		}
		while ((record = coursesTable.readLine()) != null && c > 0) {
			String[] fields = record.split(",");

			int year = Integer.parseInt(fields[0].trim().substring(0, 4));
			int month = Integer.parseInt(fields[0].trim().substring(5, 7));
			int day = Integer.parseInt(fields[0].trim().substring(8));

			Date dateAdded = new Date(year - 1900, month - 1, day);

			row.put("date_added", dateAdded);

			row.put("course_id", fields[1]);
			row.put("course_name", fields[2]);
			row.put("hours", Integer.parseInt(fields[3]));

			dbApp.insertIntoTable("courses", row);
			row.clear();

			if (limit != -1) {
				c--;
			}
		}

		coursesTable.close();
	}

	private static void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
		BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
		String record;
		int c = limit;
		if (limit == -1) {
			c = 1;
		}

		Hashtable<String, Object> row = new Hashtable<>();
		while ((record = studentsTable.readLine()) != null && c > 0) {
			String[] fields = record.split(",");

			row.put("id", fields[0]);
			row.put("first_name", fields[1]);
			row.put("last_name", fields[2]);

			int year = Integer.parseInt(fields[3].trim().substring(0, 4));
			int month = Integer.parseInt(fields[3].trim().substring(5, 7));
			int day = Integer.parseInt(fields[3].trim().substring(8));

			Date dob = new Date(year - 1900, month - 1, day);
			row.put("dob", dob);

			double gpa = Double.parseDouble(fields[4].trim());

			row.put("gpa", gpa);

			dbApp.insertIntoTable("students", row);
			row.clear();
			if (limit != -1) {
				c--;
			}
		}
		studentsTable.close();
	}

	private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
		BufferedReader transcriptsTable = new BufferedReader(
				new FileReader("src/main/resources/transcripts_table.csv"));
		String record;
		Hashtable<String, Object> row = new Hashtable<>();
		int c = limit;
		if (limit == -1) {
			c = 1;
		}
		while ((record = transcriptsTable.readLine()) != null && c > 0) {
			String[] fields = record.split(",");

			row.put("gpa", Double.parseDouble(fields[0].trim()));
			row.put("student_id", fields[1].trim());
			row.put("course_name", fields[2].trim());

			String date = fields[3].trim();
			int year = Integer.parseInt(date.substring(0, 4));
			int month = Integer.parseInt(date.substring(5, 7));
			int day = Integer.parseInt(date.substring(8));

			Date dateUsed = new Date(year - 1900, month - 1, day);
			row.put("date_passed", dateUsed);

			dbApp.insertIntoTable("transcripts", row);
			row.clear();

			if (limit != -1) {
				c--;
			}
		}

		transcriptsTable.close();
	}

	private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
		BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
		String record;
		Hashtable<String, Object> row = new Hashtable<>();
		int c = limit;
		if (limit == -1) {
			c = 1;
		}
		while ((record = pcsTable.readLine()) != null && c > 0) {
			String[] fields = record.split(",");

			row.put("pc_id", Integer.parseInt(fields[0].trim()));
			row.put("student_id", fields[1].trim());

			dbApp.insertIntoTable("pcs", row);
			row.clear();

			if (limit != -1) {
				c--;
			}
		}

		pcsTable.close();
	}

	private static void createTranscriptsTable(DBApp dbApp) throws Exception {
		// Double CK
		String tableName = "transcripts";

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("gpa", "java.lang.Double");
		htblColNameType.put("student_id", "java.lang.String");
		htblColNameType.put("course_name", "java.lang.String");
		htblColNameType.put("date_passed", "java.util.Date");

		Hashtable<String, String> minValues = new Hashtable<>();
		minValues.put("gpa", "0.7");
		minValues.put("student_id", "43-0000");
		minValues.put("course_name", "AAAAAA");
		minValues.put("date_passed", "1990-01-01");

		Hashtable<String, String> maxValues = new Hashtable<>();
		maxValues.put("gpa", "5.0");
		maxValues.put("student_id", "99-9999");
		maxValues.put("course_name", "zzzzzz");
		maxValues.put("date_passed", "2020-12-31");

		dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
	}

	private static void createStudentTable(DBApp dbApp) throws Exception {
		// String CK
		String tableName = "students";

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.String");
		htblColNameType.put("first_name", "java.lang.String");
		htblColNameType.put("last_name", "java.lang.String");
		htblColNameType.put("dob", "java.util.Date");
		htblColNameType.put("gpa", "java.lang.Double");

		Hashtable<String, String> minValues = new Hashtable<>();
		minValues.put("id", "43-0000");
		minValues.put("first_name", "AAAAAA");
		minValues.put("last_name", "AAAAAA");
		minValues.put("dob", "1990-01-01");
		minValues.put("gpa", "0.7");

		Hashtable<String, String> maxValues = new Hashtable<>();
		maxValues.put("id", "99-9999");
		maxValues.put("first_name", "zzzzzz");
		maxValues.put("last_name", "zzzzzz");
		maxValues.put("dob", "2000-12-31");
		maxValues.put("gpa", "5.0");

		dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
	}

	private static void createPCsTable(DBApp dbApp) throws Exception {
		// Integer CK
		String tableName = "pcs";

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("pc_id", "java.lang.Integer");
		htblColNameType.put("student_id", "java.lang.String");

		Hashtable<String, String> minValues = new Hashtable<>();
		minValues.put("pc_id", "0");
		minValues.put("student_id", "43-0000");

		Hashtable<String, String> maxValues = new Hashtable<>();
		maxValues.put("pc_id", "20000");
		maxValues.put("student_id", "99-9999");

		dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
	}

	private static void createCoursesTable(DBApp dbApp) throws Exception {
		// Date CK
		String tableName = "courses";

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("date_added", "java.util.Date");
		htblColNameType.put("course_id", "java.lang.String");
		htblColNameType.put("course_name", "java.lang.String");
		htblColNameType.put("hours", "java.lang.Integer");

		Hashtable<String, String> minValues = new Hashtable<>();
		minValues.put("date_added", "1901-01-01");
		minValues.put("course_id", "0000");
		minValues.put("course_name", "AAAAAA");
		minValues.put("hours", "1");

		Hashtable<String, String> maxValues = new Hashtable<>();
		maxValues.put("date_added", "2020-12-31");
		maxValues.put("course_id", "9999");
		maxValues.put("course_name", "zzzzzz");
		maxValues.put("hours", "24");

		dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

	}

	public void testWrongStudentsKeyInsertion() throws IOException {
		final DBApp dbApp = new DBApp();
		DBApp.init();

		String table = "students";
		Hashtable<String, Object> row = new Hashtable();
		row.put("id", 123);

		row.put("first_name", "foo");
		row.put("last_name", "bar");

		Date dob = new Date(1995 - 1900, 4 - 1, 1);
		row.put("dob", dob);
		row.put("gpa", 1.1);

	}

	public void testExtraTranscriptsInsertion() throws IOException {
		final DBApp dbApp = new DBApp();
		DBApp.init();

		String table = "transcripts";
		Hashtable<String, Object> row = new Hashtable();
		row.put("gpa", 1.5);
		row.put("student_id", "34-9874");
		row.put("course_name", "bar");
		row.put("elective", true);

		Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
		row.put("date_passed", date_passed);

	}

	public static void main(String[] args) throws Exception {
		DBApp db = new DBApp();
		DBApp.init();

//	        SQLTerm[] arrSQLTerms;
//	        arrSQLTerms = new SQLTerm[2];
//	        arrSQLTerms[0] = new SQLTerm();
//	        arrSQLTerms[0]._strTableName = "students";
//	        arrSQLTerms[0]._strColumnName= "first_name";
//	        arrSQLTerms[0]._strOperator = "=";
//	        arrSQLTerms[0]._objValue =row.get("first_name");
//
//	        arrSQLTerms[1] = new SQLTerm();
//	        arrSQLTerms[1]._strTableName = "students";
//	        arrSQLTerms[1]._strColumnName= "gpa";
//	        arrSQLTerms[1]._strOperator = "<=";
//	        arrSQLTerms[1]._objValue = row.get("gpa");
//
//	        String[]strarrOperators = new String[1];
//	        strarrOperators[0] = "OR";
//	      String table = "students";
//
//	        row.put("first_name", "fooooo");
//	        row.put("last_name", "baaaar");
//
//	        Date dob = new Date(1992 - 1900, 9 - 1, 8);
//	        row.put("dob", dob);
//	        row.put("gpa", 1.1);
//
//	        dbApp.updateTable(table, clusteringKey, row);
		createCoursesTable(db);
		createPCsTable(db);
		createTranscriptsTable(db);
		createStudentTable(db);
		insertPCsRecords(db, 100);
		insertTranscriptsRecords(db, 100);
		insertStudentRecords(db, 100);
		insertCoursesRecords(db, 100);
		
//        createIndex("students",new String[] {"id","first_name","last_name"});

	}

}
