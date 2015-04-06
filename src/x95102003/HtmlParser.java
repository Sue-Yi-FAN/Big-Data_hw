package x95102003;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface  HtmlParser {
	public static String getDomainName(String checkString){
		URL url;
		try {
			url = new URL(checkString);
			return url.getHost().toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return "";
		}
	}
	public static String parseHtml(String token) {
		if (token.startsWith("href=")) {
			String patternRgx = "http[s]?://.*\">";
			Matcher matcher = Pattern.compile(patternRgx).matcher(token);
			if (matcher.find()) {
				return getDomainName(matcher.group());
			}
		}
		return "";
	}
}
