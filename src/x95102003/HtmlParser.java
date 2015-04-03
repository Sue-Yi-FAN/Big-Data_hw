package x95102003;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HtmlParser {

	public HtmlParser(String inputurl) {
		// TODO Auto-generated constructor stub
	}

	public static void WriteFile(String testData) {
		try {
			BufferedWriter file = new BufferedWriter(new FileWriter("test.txt",
					true));
			file.write(testData);
			file.newLine();
			file.flush();
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String parseHtml(String token) {
		if (token.startsWith("href=")) {
			String patternRgx = "http[s]?://.*\">";
			Matcher matcher = Pattern.compile(patternRgx).matcher(token);
			if (matcher.find()) {
				try {
					URL url = new URL(matcher.group());
					WriteFile(url.getHost());
					return url.getHost();
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return "";
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			@SuppressWarnings("resource")
			BufferedReader dataHtml = new BufferedReader(new InputStreamReader(
					new FileInputStream("html100.txt"), "UTF-8"));
			String data;
			int count = 0;
			while ((data = dataHtml.readLine()) != null) {
				if (data.equals("")) {
					System.out.println("null line");
					count++;
				}
				StringTokenizer tokenizer = new StringTokenizer(data);
				while (tokenizer.hasMoreTokens()) {
					String html = tokenizer.nextToken();
					if (parseHtml(html).length() > 1) {
						// System.out.println(parseHtml(html));
						int k = 0;
					}
				}
			}
			System.out.println(count);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
