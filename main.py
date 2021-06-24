import requests
import csv
import pymysql


def people_page(page_num):
    """Функция для получение страницы персонажа с homeworld: Tatooine"""

    endpoint = 'https://swapi.dev/api/people/'

    url = endpoint + str(page_num)
    person_data = requests.get(url).json()

    if not person_data or person_data.get('detail') == 'Not found':
        return None

    homeworld_url = person_data['homeworld']
    homeworld_data = requests.get(homeworld_url).json()

    if homeworld_data['name'] != 'Tatooine':
        return []

    person = []
    if person_data['starships']:
        for ship in person_data['starships']:
            starship_data = requests.get(ship).json()

            person.append([
                person_data['name'],
                homeworld_data['name'],
                person_data['height'],
                person_data['gender'],
                starship_data['name']
            ])
    else:
        person.append([
            person_data['name'],
            homeworld_data['name'],
            person_data['height'],
            person_data['gender'],
            'n/a'
        ])
    return person


def data_aggregation():
    """Функция для агрегации всех доступных страниц персонажей с homeworld: Tatooine"""
    result_persons = []
    has_more = True
    ind = 1

    # агрегируем данные
    while has_more:
        try:
            person = people_page(ind)
        except ConnectionError:
            has_more = False
            continue

        if person is None:
            has_more = False
            continue
        ind += 1
        if person:
            result_persons.append(person)

    return result_persons



def save_pages_csv(file_name):
    """функцию для сохранения таблицы в CSV"""

    with open(file_name, 'w', newline='') as w_file:
        file_writer = csv.writer(w_file, delimiter=";")

        result_persons = data_aggregation()

        # пишем в csv
        if result_persons:
            file_writer.writerow(['name', 'homeworld.name', 'height', 'gender', 'starships.name'])
            for item in result_persons:
                file_writer.writerows(item)


def save_pages_mysql(db_name):
    """функцию для сохранения таблицы в Mysql"""
    con = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database=db_name
    )

    mycursor = con.cursor()
    star_wars_table = """
           CREATE TABLE IF NOT EXISTS starwars 
           (
               name VARCHAR(100),
               homeworld_name VARCHAR(100),
               height VARCHAR(100), 
               gender VARCHAR(100), 
               starships_name VARCHAR(100)
           )
               """
    mycursor.execute(star_wars_table)
    result_persons = data_aggregation()
    sql = """
            INSERT INTO starwars (name, homeworld_name, height, gender, starships_name)
            VALUES (%s, %s, %s, %s, %s)
          """
    for items in result_persons:
        for item in items:
            mycursor.execute(sql, item)
            con.commit()



#save_page_csv('sw.csv')
save_pages_mysql('starwars')