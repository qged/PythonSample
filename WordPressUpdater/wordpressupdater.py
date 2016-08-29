import datetime, xmlrpclib, sys
#blog url
wp_url = "https://omgtrent2sd.wordpress.com/xmlrpc.php"

#credentials for wordpress accounting owning the blog
wp_username = "qgeddes9@gmail.com"
wp_password = "superman2"

wp_blogid = ""

status_draft = 0
status_published = 1

server = xmlrpclib.ServerProxy(wp_url)

#calculating days until event
now=datetime.datetime.utcnow()
td=datetime.timedelta(seconds=(5*3600))

ttime=datetime.datetime.strptime("2015-07-27 00:01", "%Y-%m-%d %H:%M")
days=(ttime-now).days

#blog post title and content
title = "{0} Days until arrival".format(days+1)
content = "!!!"


date_created = xmlrpclib.DateTime(now)
categories = ["somecategory"]
tags = ["sometag", "othertag"]
data = {'title': title, 'description': content, 'dateCreated': date_created, 'categories': categories, 'mt_keywords': tags}

#making blog post
post_id = server.metaWeblog.newPost(wp_blogid, wp_username, wp_password, data, status_published)

sys.exit()