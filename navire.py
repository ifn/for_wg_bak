# coding: utf-8


import sqlite3
import random
import shutil
import os


DB_NAME = 'navire.db'
DB_NAME_BAK = 'navire.db.bak'


NUM_ENGINES = 6
NUM_HULLS = 5
NUM_WEAPONS = 20
NUM_SHIPS = 200


def random_integer():
    return random.randint(0, 10)


def random_text():
    return "".join(map(random.choice(['a', 'b']), range(4)))


#


def engine_name(ix):
    return "Engine-%d" % ix


def hull_name(ix):
    return "Hull-%d" % ix


def weapon_name(ix):
    return "Weapon-%d" % ix


def ship_name(ix):
    return "Ship-%d" % ix


ENGINE_NAMES = map(lambda ix: engine_name(ix), range(NUM_ENGINES))
HULL_NAMES = map(lambda ix: hull_name(ix), range(NUM_HULLS))
WEAPON_NAMES = map(lambda ix: weapon_name(ix), range(NUM_WEAPONS))
SHIP_NAMES = map(lambda ix: ship_name(ix), range(NUM_SHIPS))


def random_engine_row(engine):
    v = (engine, random_integer(), random_integer())
    return v


def random_hull_row(hull):
    v = (hull, random_integer(), random_integer(), random_integer())
    return v


def random_weapon_row(weapon):
    v = (weapon, random_integer(), random_integer(), random_integer(), random_integer(), random_integer())
    return v


def engine_fields(pk=True):
    start = int(not pk)
    return ['engine', 'power', 'type'][start:]


def hull_fields(pk=True):
    start = int(not pk)
    return ['hull', 'armor', 'type', 'capacity'][start:]


def weapon_fields(pk=True):
    start = int(not pk)
    return ['weapon', 'reload speed', 'rotational speed', 'diameter', 'power volley', 'count'][start:]


def ship_fields(pk=True):
    start = int(not pk)
    return ['ship', 'weapon', 'hull', 'engine'][start:]


def ship_row(ship, weapon, hull, engine):
    return ship, weapon, hull, engine


def random_ship_field_value(fld):
    return {
        'engine': random.choice(ENGINE_NAMES),
        'hull': random.choice(HULL_NAMES),
        'weapon': random.choice(WEAPON_NAMES),
    }[fld]


def random_fields_to_vary(tbl):
    fields = {
        'engine': engine_fields(pk=False),
        'hull': hull_fields(pk=False),
        'weapon': weapon_fields(pk=False),
    }[tbl]
    # от одного поля до почти всех неключевых
    num_fields_to_vary = random.randint(1, len(fields) - 1)
    fields_to_vary = random.sample(fields, num_fields_to_vary)
    return fields_to_vary


#


def create_db():
    conn = sqlite3.connect(DB_NAME)
    try:
        with conn:
            conn.execute('''CREATE TABLE engines (
                engine text PRIMARY KEY,
                power integer,
                type integer
            )''')

            conn.execute('''CREATE TABLE hulls (
                hull text PRIMARY KEY,
                armor integer,
                type integer,
                capacity integer
            )''')

            conn.execute('''CREATE TABLE weapons (
                weapon text PRIMARY KEY,
                [reload speed] integer,
                [rotational speed] integer,
                diameter integer,
                [power volley] integer,
                count integer
            )''')

            conn.execute('''CREATE TABLE ships (
                ship text PRIMARY KEY,
                weapon text,
                hull text,
                engine text,
                FOREIGN KEY(weapon) REFERENCES weapons(weapon),
                FOREIGN KEY(hull) REFERENCES hulls(hull),
                FOREIGN KEY(engine) REFERENCES engines(engine)
            )''')

            conn.execute('''CREATE VIEW ship_profile AS SELECT
                ships.ship, ships.weapon, ships.hull, ships.engine,
                weapons.[reload speed], weapons.[rotational speed], weapons.diameter, weapons.[power volley], weapons.count,
                hulls.armor, hulls.type, hulls.capacity,
                engines.power, engines.type
                FROM ships, weapons, hulls, engines
                WHERE ships.weapon=weapons.weapon AND ships.hull=hulls.hull AND ships.engine=engines.engine
            ''')
    finally:
        conn.close()


def ship_profile_fields():
    return [
        "ship",
        "weapon",
        "hull",
        "engine",

        "weapons.reload speed",
        "weapons.rotational speed",
        "weapons.diameter",
        "weapons.power volley",
        "weapons.count",

        "hulls.armor",
        "hulls.type",
        "hulls.capacity",

        "engines.power",
        "engines.type",
    ]


def create_data():
    conn = sqlite3.connect(DB_NAME)
    try:
        with conn:
            for engine in ENGINE_NAMES:
                r = random_engine_row(engine)
                conn.execute("INSERT INTO engines VALUES (?,?,?)", r)

            for hull in HULL_NAMES:
                r = random_hull_row(hull)
                conn.execute("INSERT INTO hulls VALUES (?,?,?,?)", r)

            for weapon in WEAPON_NAMES:
                r = random_weapon_row(weapon)
                conn.execute("INSERT INTO weapons VALUES (?,?,?,?,?,?)", r)

            for ship in SHIP_NAMES:
                engine = random.choice(ENGINE_NAMES)
                hull = random.choice(HULL_NAMES)
                weapon = random.choice(WEAPON_NAMES)
                r = ship_row(ship, weapon, hull, engine)
                conn.execute("INSERT INTO ships VALUES (?,?,?,?)", r)
    finally:
        conn.close()


def change_data():
    conn = sqlite3.connect(DB_NAME)
    try:
        with conn:
            for engine in ENGINE_NAMES:
                fields_to_vary = random_fields_to_vary('engine')
                for fld in fields_to_vary:
                    val = random_integer()
                    conn.execute("UPDATE engines SET [{fld}]=? WHERE engine=?".format(fld=fld), (val, engine))

            for hull in HULL_NAMES:
                fields_to_vary = random_fields_to_vary('hull')
                for fld in fields_to_vary:
                    val = random_integer()
                    conn.execute("UPDATE hulls SET [{fld}]=? WHERE hull=?".format(fld=fld), (val, hull))

            for weapon in WEAPON_NAMES:
                fields_to_vary = random_fields_to_vary('weapon')
                for fld in fields_to_vary:
                    val = random_integer()
                    conn.execute("UPDATE weapons SET [{fld}]=? WHERE weapon=?".format(fld=fld), (val, weapon))

            for ship in SHIP_NAMES:
                fld = random.choice(ship_fields(pk=False))
                val = random_ship_field_value(fld)
                conn.execute("UPDATE ships SET [{fld}]=? WHERE ship=?".format(fld=fld), (val, ship))
    finally:
        conn.close()


def create_bak():
    shutil.copy(DB_NAME, DB_NAME_BAK)


def clean_workspace():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    if os.path.exists(DB_NAME_BAK):
        os.remove(DB_NAME_BAK)


#


def setup():
    clean_workspace()
    create_db()
    create_data()
    create_bak()
    change_data()


def check_ship(ship, ship_profile_old, ship_profile):
    assert ship == ship_profile_old[0]
    assert ship == ship_profile[0]

    for ix, fld in enumerate(ship_profile_fields()):
        v_old = ship_profile_old[ix]
        v = ship_profile[ix]

        assert v_old == v, "%s mismatch. Expected: %s, was: %s" % (fld, v_old, v)


def test_ships():
    conn = sqlite3.connect(DB_NAME)
    conn_bak = sqlite3.connect(DB_NAME_BAK)
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM ship_profile")

        c_bak = conn_bak.cursor()
        c_bak.execute("SELECT * FROM ship_profile")

        for ship in SHIP_NAMES:
            ship_profile_old = c_bak.fetchone()
            ship_profile = c.fetchone()

            yield check_ship, ship, ship_profile_old, ship_profile
    finally:
        conn.close()
        conn_bak.close()
