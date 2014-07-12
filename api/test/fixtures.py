atom10 = """
<?xml version="1.0" encoding="utf-8"?>

<feed xmlns="http://www.w3.org/2005/Atom">

	<title>Example Feed</title>
	<subtitle>A subtitle.</subtitle>
	<link href="http://example.org/feed/" rel="self" />
	<link href="http://example.org/" />
	<id>urn:uuid:60a76c80-d399-11d9-b91C-0003939e0af6</id>
	<updated>2003-12-13T18:30:02Z</updated>


	<entry>
		<title>Atom-Powered Robots Run Amok</title>
		<link href="http://example.org/2003/12/13/atom03" />
		<link rel="alternate" type="text/html" href="http://example.org/2003/12/13/atom03.html"/>
		<link rel="edit" href="http://example.org/2003/12/13/atom03/edit"/>
		<id>urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</id>
		<updated>2003-12-13T18:30:02Z</updated>
		<summary>Some text.</summary>
                <content type="xhtml">
                   <div xmlns="http://www.w3.org/1999/xhtml">
                      <p>This is the entry content.</p>
                   </div>
                </content>
                <author>
                      <name>John Doe</name>
                      <email>johndoe@example.com</email>
               </author>
	</entry>

</feed>
"""

atom03 = """
<?xml version="1.0"?>
<feed version="0.3" xmlns="http://purl.org/atom/ns#"
xmlns:g="http://base.google.com/ns/1.0">
<title>The name of your data feed</title>
<link href="http://www.example.com" rel="alternate" type="text/html" />
<modified>2005-10-11T18:30:02Z</modified>
<author>
<name>Google</name>
</author>
<id>tag:google.com,2005-10-15:/support/products</id>
<entry>
<title>Red wool sweater</title>
<link href="http://www.example.com/item1-info-page.html" />
<summary>Comfortable and soft, this sweater will keep you warm on those cold winter nights.</summary>
<id>tag:google.com,2005-10-15:/support/products</id>
<issued>2005-10-13T18:30:02Z</issued>
<modified>2005-10-13T18:30:02Z</modified>
<g:image_link>http://www.example.com/image1.jpg</g:image_link>
<g:price>25</g:price>
<g:condition>new</g:condition>
</entry>
</feed>
"""

rss20 = """
<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
<channel>
 <title>RSS Title</title>
 <description>This is an example of an RSS feed</description>
 <link>http://www.example.com/main.html</link>
 <lastBuildDate>Mon, 06 Sep 2010 00:01:00 +0000 </lastBuildDate>
 <pubDate>Sun, 06 Sep 2009 16:20:00 +0000</pubDate>
 <ttl>1800</ttl>

 <item>
  <title>Example entry</title>
  <description>Here is some text containing an interesting description.</description>
  <link>http://www.example.com/blog/post/1</link>
  <guid>7bd204c6-1655-4c27-aeee-53f933c5395f</guid>
  <pubDate>Sun, 06 Sep 2009 16:20:00 +0000</pubDate>
 </item>

</channel>
</rss>
"""

html5 = """
<!doctype HTML>
<html>
    <head>
    </head>
    <body>
        <a href='http://www.buzzfeed.com/username/post-slug'>absolute url</a>
        <a href='/username/post-slug'>relative url</a>
        <a href='javascript:;'>event target</a>
        <a href='javascript: void(0);'>alternate event target</a>
        <a href='/'>document root</a>
        <a>Non-existant href</a>
    </body>
</html>
"""
