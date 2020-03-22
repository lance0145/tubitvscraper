# Libraries
import os
import requests
import sys
import re
import time
import datetime
import psycopg2
import traceback
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from codecs import open
from datetime import datetime, date, timedelta

# ====================================================================Parse tubi data section===========================================================================
# Define variables needed
today = date.today()
start_time = time.time()
urls = []
contents = []
details = []
rowcount = 0
imdb_number = ""

try:
	options = Options()
	options.add_argument('-headless')
	driver = webdriver.Firefox(executable_path='geckodriver', options=options)
	driver.get("https://tubitv.com")
except Exception as e:
	logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")

# Auto create folder and files if not exist
logs_str = "Desktop/Works/tubitv/logs/logs_" + today.strftime('%Y.%m.%d') + ".log"
logs_exist = False
logs_int = 0
if not os.path.exists(logs_str):
	os.makedirs(os.path.dirname(logs_str), 0o777, True)
	logs = open(logs_str, "w", encoding='utf-8')
	logs_exist = True
while (logs_exist == False):
	logs_int += 1
	logs_str = "Desktop/Works/tubitv/logs/logs_" + today.strftime('%Y.%m.%d') + "_" + str(logs_int) + ".log"
	if not os.path.exists(logs_str):
		os.makedirs(os.path.dirname(logs_str), 0o777, True)
		logs = open(logs_str, "w", encoding='utf-8')
		logs_exist = True

data_str = "Desktop/Works/tubitv/data/cc_output_" + today.strftime('%Y.%m.%d') + ".csv"
data_exist = False
data_int = 0
if not os.path.exists(data_str):
	os.makedirs(os.path.dirname(data_str), 0o777, True)
	data = open(data_str, "w", encoding='utf-8')
	data_exist = True
while (data_exist == False):
	data_int += 1
	data_str = "Desktop/Works/tubitv/data/cc_output_" + today.strftime('%Y.%m.%d') + "_" + str(data_int) + ".csv"
	if not os.path.exists(data_str):
		os.makedirs(os.path.dirname(data_str), 0o777, True)
		data = open(data_str, "w", encoding='utf-8')
		data_exist = True

data_str = "Desktop/Works/tubitv/data/cc_output_detail_" + today.strftime('%Y.%m.%d') + ".csv"
data_exist = False
data_int = 0
if not os.path.exists(data_str):
	os.makedirs(os.path.dirname(data_str), 0o777, True)
	data_detail = open(data_str, "w", encoding='utf-8')
	data_exist = True
while (data_exist == False):
	data_int += 1
	data_str = "Desktop/Works/tubitv/data/cc_output_detail_" + today.strftime('%Y.%m.%d') + "_" + str(data_int) + ".csv"
	if not os.path.exists(data_str):
		os.makedirs(os.path.dirname(data_str), 0o777, True)
		data_detail = open(data_str, "w", encoding='utf-8')
		data_exist = True

data_str = "Desktop/Works/tubitv/data/cc_output_series_" + today.strftime('%Y.%m.%d') + ".csv"
data_exist = False
data_int = 0
if not os.path.exists(data_str):
	os.makedirs(os.path.dirname(data_str), 0o777, True)
	data_series = open(data_str, "w", encoding='utf-8')
	data_exist = True
while (data_exist == False):
	data_int += 1
	data_str = "Desktop/Works/tubitv/data/cc_output_series_" + today.strftime('%Y.%m.%d') + "_" + str(data_int) + ".csv"
	if not os.path.exists(data_str):
		os.makedirs(os.path.dirname(data_str), 0o777, True)
		data_series = open(data_str, "w", encoding='utf-8')
		data_exist = True

html_str = "Desktop/Works/tubitv/html/tubitv_" + today.strftime('%Y.%m.%d') + ".html"
html_exist = False
html_int = 0
if not os.path.exists(html_str):
	os.makedirs(os.path.dirname(html_str), 0o777, True)
	html = open(html_str, "w", encoding='utf-8')
	html_exist = True
while (html_exist == False):
	html_int += 1
	html_str = "Desktop/Works/tubitv/html/tubitv_" + today.strftime('%Y.%m.%d') + "_" + str(html_int) + ".html"
	if not os.path.exists(html_str):
		os.makedirs(os.path.dirname(html_str), 0o777, True)
		html = open(html_str, "w", encoding='utf-8')
		html_exist = True

# Initialize database
try:
	con = psycopg2.connect("")
	cur = con.cursor()
except Exception as e:
	logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")

# Robot parse started
try:
	day = date.today()
	now = datetime.now().strftime("%H:%M:%S")
	logs.write("date_time, events_errors & traceback\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Robot start time\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Command = python3 " + sys.argv[0] + "\n")
	data.write("date|time|robot|category|title|year|runtime|rating|genre_01|genre_02|genre_03|genre_04|genre_05|imdb_number|tv_or_movie|tvm_manually_checked|url_detail|tubi_number\n")
	data.write(str(day) + "|" + now + "|" + "Robot start time\n")
	data.write(str(day) + "|" + now + "|" + "Command = python3 " + sys.argv[0] + "\n")
	cur.execute("INSERT INTO online_tubi(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Robot start time"))
	cur.execute("INSERT INTO online_tubi(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Command = python3 " + sys.argv[0]))
	con.commit()
except Exception as e:
	logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")

# Loading animation
def animate():
	sys.stdout.write('\rLoading /')
	time.sleep(0.07)
	sys.stdout.write('\rLoading -')
	time.sleep(0.07)
	sys.stdout.write('\rLoading \\')
	time.sleep(0.07)
	sys.stdout.write('\rLoading |')
	time.sleep(0.07)

print("Parsing https://www.tubitv.com")
print("=> Collecting links")
element = driver.find_element_by_css_selector("#app > div._3Qo9r > header > div.Container._3JvwP > div._1sZ9q > div > div.Ajer5")
crawl = ActionChains(driver).move_to_element(element)
crawl.perform()
time.sleep(2)
links = driver.find_elements_by_class_name("ATag")
for l in links:
	l.find_elements_by_tag_name('a')
	url = l.get_attribute("href")
	if ("category" not in url) and ("channels" not in url):
		break
	urls.append(url)
	#animate()
sys.stdout.write('\rDone      \n')

print("=> Saving html")
#for u in range(5):
for u in range(len(urls)):
	try:
		driver.get(urls[u])
	except Exception as e:
		logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
	height = 0
	bottom = False
	while(bottom == False):
		scrolled = driver.execute_script("window.scrollBy(0, 400); var scrolled = window.pageYOffset; return scrolled;")
		time.sleep(.5)
		if scrolled == height:
			bottom = True
		height = scrolled
		#animate()
	contents.append(driver.page_source)
	html.write(driver.page_source)
	#html.write(driver.page_source + "\n")
sys.stdout.write('\rDone      \n')

print("=> Writing to file and database")
for c in range(len(contents)):
	soup = BeautifulSoup(contents[c], 'lxml')
	try:
		category = soup.find("h1", {"class" : "_2FHnJ"}).get_text()
	except:
		try:
			category = soup.find("div", {"class" : "ThvW6"}).get_text()
		except:
			category = ""
	item = soup.findAll("div",{"JB9zq"})
	runtime = 0
	for i in range(len(item)):
		try:
			title = item[i].find("a", {"class" : "ATag zIZVd"}).get_text()
			title_link = item[i].find("a", {"class" : "ATag zIZVd"})
			url_detail = "https://tubitv.com" + title_link["href"]
			tubi_number = re.findall("\d+", url_detail)
		except:
			title = ""
		try:
			year = item[i].find("div", {"class" : "_3BhXb"}).get_text()
		except:
			year = ""
		try:
			run = item[i].find("div", {"class" : "yPcUu"}).get_text()
			tv_or_movie = "Movie"
			if "hr" in run and "min" in run:
				run = run.replace(" hr","")
				run = run.replace(" min","")
				runs = run.split()
				runs = [int(i) for i in runs]
				runtime = (runs[0] * 60) + runs[1]
			elif "hr" in run and "min" not in run:
				run = run.replace(" hr","")
				runs = run.split()
				runs = [int(i) for i in runs]
				runtime = runs[0] * 60
			else:
				run = run.replace(" min","")
				runs = run.split()
				runs = [int(i) for i in runs]
				runtime = runs[0]
		except:
			runtime = 0
			tv_or_movie = "TV"
		try:
			rating = item[i].find("div", {"class" : "_30bN1 _2x-ll"}).get_text()
		except:
			rating = ""
		try:
			genre = item[i].find("div", {"class" : "RmVOo _27rH2"}).get_text()
			genres = genre.split(", ")
			if len(genres) == 1:
				genres.append("")
				genres.append("")
			elif len(genres) == 2:
				genres.append("")
		except:
			genres = ["","",""]
		if title != "":
			day = date.today()
			now = datetime.now().strftime("%H:%M:%S")
			try:
				data.write(str(day) + "|" + now + "||" + category + "|" + title + "|" + year.strip("()") + "|" + str(runtime) + "|" + rating + "|" + genres[0] + "|" + genres[1] + "|" + genres[2] + "||||" + tv_or_movie + "||" + url_detail + "|" + str(tubi_number[0]) + "\n")
			except Exception as e:
				logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
			try:
				cur.execute("INSERT INTO online_tubi(date, time, category, title, year, runtime, rating, genre_01, genre_02, genre_03, tv_or_movie, url_detail, tubi_number) \
				VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
				(day, now, category, title, year.strip("()"), runtime, rating, genres[0], genres[1], genres[2], tv_or_movie, url_detail, int(tubi_number[0])))
				con.commit()
				rowcount += 1
				details.append([url_detail, title, year.strip("()"), runtime, rating, genres[0], genres[1], genres[2], imdb_number, tubi_number[0], tv_or_movie])
			except Exception as e:
				logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
			#animate()
sys.stdout.write('\rDone      \n')

# Finished
finish_time = str(timedelta(seconds = time.time() - start_time)).split(".")[0]
print("Elapsed time", finish_time)
print("Total record inserted", rowcount)
try:
	day = date.today()
	now = datetime.now().strftime("%H:%M:%S")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Total record inserted "  + str(rowcount) + "\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Elapsed time " + finish_time + "\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Robot end time\n")
	data.write(str(day) + "|" + now + "|" + "Total record inserted "  + str(rowcount) + "\n")
	data.write(str(day) + "|" + now + "|" + "Elapsed time " + finish_time + "\n")
	data.write(str(day) + "|" + now + "|" + "Robot end time\n")
	cur.execute("INSERT INTO online_tubi(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Total record inserted "  + str(rowcount)))
	cur.execute("INSERT INTO online_tubi(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Elapsed time " + finish_time))
	cur.execute("INSERT INTO online_tubi(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Robot end time"))
	con.commit()
except Exception as e:
	logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")

# ====================================================================Parse tubi details and series section===========================================================================
# Define variables needed
today = date.today()
start_time = time.time()
details_content = []
rowcount = 0
rowcount_series = 0
series = []
actors = []

try:
	day = date.today()
	now = datetime.now().strftime("%H:%M:%S")
	logs.write("\n" + str(datetime.now().replace(microsecond=0)) + ", Robot details and series start time\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Command = python3 " + sys.argv[0] + "\n")
	data_detail.write("date|time|robot|title|year|runtime|close_caption|rating|genre_01|genre_02|genre_03|genre_04|genre_05|description|director|actor_01|actor_02|actor_03|actor_04|actor_05|actor_06|actor_07|actor_08|actor_09|actor_10|actor_11|actor_12|actor_13|actor_14|actor_15|imdb_number|tubi_number\n")
	data_detail.write(str(day) + "|" + now + "|" + "Robot start time\n")
	data_detail.write(str(day) + "|" + now + "|" + "Command = python3 " + sys.argv[0] + "\n")
	data_series.write("date|time|robot|season_number|episode_number|episode_name|episode_description|imdb_number|tubi_number\n")
	data_series.write(str(day) + "|" + now + "|" + "Robot start time\n")
	data_series.write(str(day) + "|" + now + "|" + "Command = python3 " + sys.argv[0] + "\n")
	cur.execute("INSERT INTO online_tubi_detail(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Robot start time"))
	cur.execute("INSERT INTO online_tubi_detail(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Command = python3 " + sys.argv[0]))
	cur.execute("INSERT INTO online_tubi_series(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Robot start time"))
	cur.execute("INSERT INTO online_tubi_series(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Command = python3 " + sys.argv[0]))
	con.commit()
except Exception as e:
	logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")

def get_episodes(episode):
	episodes = episode.split("\n")
	if len(episodes) >= 2:
		episode_description = episodes[1]
		episode_number_names = episodes[0].replace(": ", "", 1)
		episode_number_name = episode_number_names.replace(" :", "", 1)
		season_episode_number = episode_number_name.split(" ", 1)
		if len(season_episode_number) >= 2:
			episode_numbers = re.findall("\d+", season_episode_number[0])
			episode_name = season_episode_number[1].replace("- ", "", 1)
			if len(episode_numbers) >= 2:
				season_number = episode_numbers[0]
				episode_number = episode_numbers[1]
				day = date.today()
				now = datetime.now().strftime("%H:%M:%S")
				try:
					data_series.write(str(day) + "|" + now + "||" + str(season_number) + "|"  + str(episode_number) + "|" + episode_name + "|" + episode_description + "||" + str(details[d][9]) + "\n")
				except Exception as e:
					logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
				try:
					cur.execute("INSERT INTO online_tubi_series(date, time, season_number, episode_number, episode_name, episode_description, tubi_number) \
					VALUES(%s, %s, %s, %s, %s, %s, %s)", \
					(day, now, int(season_number), int(episode_number), episode_name, episode_description, details[d][9]))
					con.commit()
					global rowcount_series
					rowcount_series += 1
				except Exception as e:
					logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
			else:
				logs.write(str(datetime.now().replace(microsecond=0)) + ", To check " + str(episode) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")
		else:
			logs.write(str(datetime.now().replace(microsecond=0)) + ", To check " + str(episode) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")
	else:
		logs.write(str(datetime.now().replace(microsecond=0)) + ", To check " + str(episode) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")

print("\nParsing https://www.tubitv.com details and series")
print("=> Collecting details and series")
for d in range(len(details)):
	try:
		driver.get(details[d][0])
	except Exception as e:
		logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
	driver.execute_script("window.scrollBy(0, 400)")
	soup = BeautifulSoup(driver.page_source, 'lxml')
	html.write(driver.page_source)
	details_content.append([driver.page_source, details[d][1], details[d][2], details[d][3], details[d][4], details[d][5], details[d][6], details[d][7], details[d][8], details[d][9], details[d][10]])
	if details[d][10] == "TV":
		season = soup.find("h3", {"class" : "_3hqTw"})
		if season == None:
			driver.execute_script("window.scrollBy(0, 400)")
			try:
				driver.execute_script("document.getElementsByClassName('Select__down-icon')[0].click()")
				element = driver.find_element_by_class_name("Select__list")
				el = element.text
				elements = el.split("\n")
				for e in elements:
					argument = "li[data-value='" + e + "']"
					driver.execute_script("document.getElementsByClassName('Select__down-icon')[0].click()")
					driver.execute_script("document.querySelector(arguments[0]).click()", argument)
					items = driver.execute_script("var items = document.getElementsByClassName('Col Col--6 Col--lg-4 W7v7G'); return items;")
					int_itm = len(items) / 3
					remainder = len(items) % 3
					item = int(int_itm)
					for i in range(item):
						time.sleep(.5)
						items = driver.execute_script("var items = document.getElementsByClassName('Col Col--6 Col--lg-4 W7v7G'); return items;")
						for it in range(3):
							array_num = i * 3 + it
							episode = items[array_num].text
							get_episodes(episode)
						try:
							driver.execute_script('document.getElementsByClassName("Button Button--secondary Button--round Carousel__next Carousel__arrow-active")[0].click()')
						except:
							pass
					if remainder != 0:
						time.sleep(.5)
						items = driver.execute_script("var items = document.getElementsByClassName('Col Col--6 Col--lg-4 W7v7G'); return items;")
						if remainder == 2:
							episode = items[len(items) - 2].text
							get_episodes(episode)
							episode = items[len(items) - 1].text
							get_episodes(episode)
						else:
							episode = items[len(items) - 1].text
							get_episodes(episode)
			except Exception as e:
				logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")
			#animate()
		else:
			item = soup.findAll("div", {"class" : "Col Col--6 Col--lg-4 W7v7G"})
			for i in range(len(item)):
				try:
					episodes = item[i].find("div", {"class" : "_1g4Iu"}).get_text()
					episode_number_names = episodes.replace(": ", "", 1)
					episode_number_name = episode_number_names.replace(" :", "", 1)
					season_episode_number = episode_number_name.split(" ", 1)
					if len(season_episode_number) >= 2:
						episode_numbers = re.findall("\d+", season_episode_number[0])
						episode_name = season_episode_number[1].replace("- ", "", 1)
						if len(episode_numbers) >= 2:
							season_number = episode_numbers[0]
							episode_number = episode_numbers[1]
							episode_name = season_episode_number[1]
							try:
								episode_description = item[i].find("div", {"class" : "_2GgQ0"}).get_text()
							except:
								episode_description = ""
							day = date.today()
							now = datetime.now().strftime("%H:%M:%S")
							try:
								data_series.write(str(day) + "|" + now + "||" + str(season_number) + "|" + str(episode_number) + "|" + episode_name + "|" + episode_description + "||" + str(details[d][9]) + "\n")
							except Exception as e:
								logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
							try:
								cur.execute("INSERT INTO online_tubi_series(date, time, season_number, episode_number, episode_name, episode_description, tubi_number) \
								VALUES(%s, %s, %s, %s, %s, %s, %s)", \
								(day, now, int(season_number), int(episode_number), episode_name, episode_description, details[d][9]))
								con.commit()
								rowcount_series += 1
							except Exception as e:
								logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
						else:
							logs.write(str(datetime.now().replace(microsecond=0)) + ", To check " + str(episode) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")
					else:
						logs.write(str(datetime.now().replace(microsecond=0)) + ", To check " + str(episode) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")
				except Exception as e:
					logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + details[d][9] + ", " + traceback.format_exc() + "\n")
				#animate()
sys.stdout.write('\rDone      \n')

print("=> Writing details and series to file and database")
for dc in range(len(details_content)):
	soup = BeautifulSoup(details_content[dc][0], 'lxml')
	item = soup.findAll("div",{"Container _3KtU0"})
	try:
		cc = soup.find("svg", {"class" : "_2rWrR _3zBGD"})
		close_caption = "Yes"
	except:
		close_caption = "No"
	try:
		description = soup.find("div", {"class" : "_1_hc6"}).get_text()
	except:
		description = ""
	try:
		directors = soup.findAll("div", {"class" : "_1yVRz"})
		director = directors[0].text
	except:
		director = ""
	try:
		actor = soup.findAll("div", {"class" : "_2-JB4 _3T5QT"})
		for a in actor:
			actors.append(a.text)
		actors_diff = 11 - len(actor)
		for a in actors_diff:
			actors.append("")
	except:
		for a in range(11):
			actors.append("")
	day = date.today()
	now = datetime.now().strftime("%H:%M:%S")
	try:
		data_detail.write(str(day) + "|" + now + "||" + details_content[dc][1] + "|" + details_content[dc][2] + "|" + str(details_content[dc][3]) + "|" + close_caption + "|" + details_content[dc][4] + "|" + details_content[dc][5] + "|" + details_content[dc][6] + "|" + details_content[dc][7] + "|||" + description + "|" + director + "|" + actors[1] + "|" + actors[2] + "|" + actors[3] + "|" + actors[4] + "|" + actors[5] + "|" + actors[6] + "|" + actors[7] + "|" + actors[8] + "|" + actors[9] + "|" + actors[10] + "|||||||" + str(details_content[dc][9]) + "\n")
	except Exception as e:
		logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
	try:
		cur.execute("INSERT INTO online_tubi_detail(date, time, title, year, runtime, close_caption, rating, genre_01, genre_02, genre_03, description, director, actor_01, actor_02, actor_03, actor_04, actor_05, actor_06, actor_07, actor_08, actor_09, actor_10, tubi_number) \
		VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (tubi_number) DO NOTHING", \
		(day, now, details_content[dc][1], details_content[dc][2], details_content[dc][3], close_caption, details_content[dc][4], details_content[dc][5], details_content[dc][6], description, director, actors[1], actors[2], actors[3], actors[4], actors[5], actors[6], actors[7], actors[8], actors[9], actors[10], details_content[dc][8], details_content[dc][9]))
		con.commit()
		rowcount += 1
	except Exception as e:
		logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")
	#animate()
sys.stdout.write('\rDone      \n')

# Finished
finish_time = str(timedelta(seconds = time.time() - start_time)).split(".")[0]
print("Elapsed time", finish_time)
print("Total details record process", rowcount)
print("Total series record inserted", rowcount_series)
try:
	day = date.today()
	now = datetime.now().strftime("%H:%M:%S")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Total details record process "  + str(rowcount) + "\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Total series record inserted "  + str(rowcount_series) + "\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Elapsed time " + finish_time + "\n")
	logs.write(str(datetime.now().replace(microsecond=0)) + ", Robot end time\n")
	data_detail.write(str(day) + "|" + now + "|" + "Total record process "  + str(rowcount) + "\n")
	data_detail.write(str(day) + "|" + now + "|" + "Elapsed time " + finish_time + "\n")
	data_detail.write(str(day) + "|" + now + "|" + "Robot end time\n")
	data_series.write(str(day) + "|" + now + "|" + "Total record inserted "  + str(rowcount_series) + "\n")
	data_series.write(str(day) + "|" + now + "|" + "Elapsed time " + finish_time + "\n")
	data_series.write(str(day) + "|" + now + "|" + "Robot end time\n")
	cur.execute("INSERT INTO online_tubi_detail(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Total record process "  + str(rowcount)))
	cur.execute("INSERT INTO online_tubi_detail(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Elapsed time " + finish_time))
	cur.execute("INSERT INTO online_tubi_detail(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Robot end time"))
	cur.execute("INSERT INTO online_tubi_series(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Total record inserted "  + str(rowcount_series)))
	cur.execute("INSERT INTO online_tubi_series(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Elapsed time " + finish_time))
	cur.execute("INSERT INTO online_tubi_series(date, time, robot) VALUES(%s, %s, %s)", (day, now, "Robot end time"))
	con.commit()
except Exception as e:
	logs.write(str(datetime.now().replace(microsecond=0)) + ", " + str(e) + ", " + traceback.format_exc() + "\n")

# Close browser, database & files 
driver.quit()
con.close()
cur.close()
logs.close()
data.close()
data_detail.close()
data_series.close()
html.close()
