#程式說明
	將巨量網站的檔案，分解後算出其Page Rank
	因為格式修正因此不需要BlockRecordReader的方式，
	只需要使用預設的TextInputFormat一行一行讀．
###輸入格式###
	File內容
		weburl @#*#@ <html>
		weburl @#*#@ <div> aaa </div>	
		weburl @#*#@ <a> href="http://domain" </a>
		weburl @#*#@ </html>
	使用內建的LineInputFormat即可
###實作方式：
	以 "@#*#@"符號做切割，
	將key設為weburl的domain
	而當內容具有href="http://"將domain 設為value
	當作Mapper 產生的pair(key,value)
	Reducer 負責將key整合，並將value重複去除，
	將pageRank初始為 1

##參考
*[Morris gitHub](https://github.com/morris821028/hw-BigData)

*[Map Reduce觀念](http://www.slideshare.net/waue/hadoop-map-reduce-3019713)

*[BlockInputFormat方式](http://bigdatacircus.com/2012/08/01/wordcount-with-custom-record-reader-of-textinputformat/)