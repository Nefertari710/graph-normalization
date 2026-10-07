import random
import datetime

def make_params_q1(seed, runs):
    rng = random.Random(seed)
    return [
        {"delta": rng.randint(60, 120)}
        for _ in range(runs)
    ]

def make_params_q2(seed, runs):
    rng = random.Random(seed)
    syllable_3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

    params = []

    for _ in range(runs):
        params.append({
            "p_size": rng.randint(1, 50),
            "type_suffix": rng.choice(syllable_3),
            "region": rng.choice(regions)
        })

    return params

def make_params_q3(seed, runs):
    """
    TPC-H Query 3 parameter generator.

    Parameters:
    - SEGMENT ∈ Market Segments
    - DATE ∈ [1995-03-01 .. 1995-03-31]
    """
    rng = random.Random(seed)

    segments = [
        "AUTOMOBILE",
        "BUILDING",
        "FURNITURE",
        "MACHINERY",
        "HOUSEHOLD"
    ]

    start_date = datetime.date(1995, 3, 1)
    end_date   = datetime.date(1995, 3, 31)
    day_range  = (end_date - start_date).days + 1  # inclusive

    params = []

    for _ in range(runs):
        segment = rng.choice(segments)

        offset = rng.randint(0, day_range - 1)
        date = start_date + datetime.timedelta(days=offset)
        params.append({
            "segment": segment,
            "date": date.isoformat()
        })
    return params

def make_params_q4(seed, runs):
    """
    TPC-H Query 4 parameter generator.

    DATE is the first day of a randomly selected month
    between 1993-01-01 and 1997-10-01 (inclusive).
    """
    rng = random.Random(seed)

    start_year, start_month = 1993, 1
    end_year, end_month     = 1997, 10

    total_months = (end_year - start_year) * 12 + (end_month - start_month) + 1

    params = []

    for _ in range(runs):
        offset = rng.randint(0, total_months - 1)

        year  = start_year + (start_month - 1 + offset) // 12
        month = (start_month - 1 + offset) % 12 + 1

        date = datetime.date(year, month, 1)

        params.append({
            "date": date.isoformat()
        })

    return params

def make_params_q5(seed, runs):
    """
    TPC-H Query 5 parameter generator.

    REGION ∈ R_NAME
    DATE   = January 1st of a randomly selected year in [1993 .. 1997]
    """
    rng = random.Random(seed)

    regions = [
        "AFRICA",
        "AMERICA",
        "ASIA",
        "EUROPE",
        "MIDDLE EAST"
    ]

    years = list(range(1993, 1998))  # inclusive

    params = []

    for _ in range(runs):
        region = rng.choice(regions)
        year   = rng.choice(years)

        date = datetime.date(year, 1, 1)

        params.append({
            "region": region,
            "date": date.isoformat()
        })

    return params

def make_params_q6(seed, runs):
    """
    TPC-H Query 6 parameter generator.
    """
    rng = random.Random(seed)

    years = list(range(1993, 1998))  # [1993..1997]

    params = []

    for _ in range(runs):
        # DATE
        year = rng.choice(years)
        date = datetime.date(year, 1, 1)

        # DISCOUNT
        discount = rng.uniform(0.02, 0.09)
        discount = round(discount, 2)

        # QUANTITY
        quantity = rng.randint(24, 25)

        params.append({
            "date": date.isoformat(),
            "discount": discount,
            "quantity": quantity
        })

    return params

def make_params_q7(seed, runs):
    """
    TPC-H Query 7 parameter generator.

    Parameters:
    - NATION1 ∈ N_NAME
    - NATION2 ∈ N_NAME
    - NATION1 != NATION2
    """
    rng = random.Random(seed)

    nations = [
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
        "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
        "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
        "SAUDI ARABIA", "VIETNAM", "RUSSIA",
        "UNITED KINGDOM", "UNITED STATES"
    ]

    params = []

    for _ in range(runs):
        n1, n2 = rng.sample(nations, 2)

        params.append({
            "nation1": n1,
            "nation2": n2
        })

    return params

def make_params_q8(seed, runs):
    """
    TPC-H Query 8 parameter generator (规范级正确).

    Parameters:
    - nation : N_NAME
    - region : region of nation
    - ptype  : 3-syllable part type
    """

    # =========================
    # Constants (Q8 only)
    # =========================
    REGION_NAMES = {
        0: "AFRICA",
        1: "AMERICA",
        2: "ASIA",
        3: "EUROPE",
        4: "MIDDLE EAST",
    }

    NATION_REGIONKEY = {
        "ALGERIA": 0,
        "ARGENTINA": 1,
        "BRAZIL": 1,
        "CANADA": 1,
        "EGYPT": 4,
        "ETHIOPIA": 0,
        "FRANCE": 3,
        "GERMANY": 3,
        "INDIA": 2,
        "INDONESIA": 2,
        "IRAN": 4,
        "IRAQ": 4,
        "JAPAN": 2,
        "JORDAN": 4,
        "KENYA": 0,
        "MOROCCO": 0,
        "MOZAMBIQUE": 0,
        "PERU": 1,
        "CHINA": 2,
        "ROMANIA": 3,
        "SAUDI ARABIA": 4,
        "VIETNAM": 2,
        "RUSSIA": 3,
        "UNITED KINGDOM": 3,
        "UNITED STATES": 1,
    }

    # =========================
    # Helper functions (local)
    # =========================
    def nation_to_region_name(nation: str) -> str:
        return REGION_NAMES[NATION_REGIONKEY[nation]]

    def generate_types_3_syllable():
        syllable1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
        syllable2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
        syllable3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

        return [
            f"{a} {b} {c}"
            for a in syllable1
            for b in syllable2
            for c in syllable3
        ]

    # =========================
    # Parameter generation
    # =========================
    rng = random.Random(seed)

    nations = list(NATION_REGIONKEY.keys())
    types_3_syllable = generate_types_3_syllable()  # 150

    params = []

    for _ in range(runs):
        nation = rng.choice(nations)
        region = nation_to_region_name(nation)
        ptype = rng.choice(types_3_syllable)

        params.append({
            "nation": nation,
            "region": region,
            "ptype": ptype
        })

    return params

def make_params_q9(seed, runs):
    """
    TPC-H Query 9 parameter generator.

    COLOR is randomly selected from the list of color values
    used in the generation of P_NAME.

    Used as:
        p_name LIKE '%COLOR%'
    """
    rng = random.Random(seed)

    colors = [
        "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
        "blanched", "blue", "blush", "brown", "burlywood", "burnished",
        "chartreuse", "chiffon", "chocolate", "coral", "cornflower",
        "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger",
        "drab", "firebrick", "floral", "forest", "frosted", "gainsboro",
        "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian",
        "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light",
        "lime", "linen", "magenta", "maroon", "medium", "metallic",
        "midnight", "mint", "misty", "moccasin", "navajo", "navy",
        "olive", "orange", "orchid", "pale", "papaya", "peach", "peru",
        "pink", "plum", "powder", "puff", "purple", "red", "rose",
        "rosy", "royal", "saddle", "salmon", "sandy", "seashell",
        "sienna", "sky", "slate", "smoke", "snow", "spring", "steel",
        "tan", "thistle", "tomato", "turquoise", "violet", "wheat",
        "white", "yellow"
    ]

    params = []

    for _ in range(runs):
        color = rng.choice(colors)
        params.append({
            "color": color
        })

    return params

def make_params_q10(seed, runs):
    """
    TPC-H Query 10 parameter generator.

    DATE is the first day of a randomly selected month
    from February 1993 to January 1995 (inclusive).

    Used as:
        o_orderdate >= DATE
        o_orderdate < DATE + INTERVAL '3 months'
    """
    rng = random.Random(seed)

    start_year, start_month = 1993, 2   # 1993-02
    end_year, end_month     = 1995, 1   # 1995-01

    total_months = (end_year - start_year) * 12 + (end_month - start_month) + 1

    params = []

    for _ in range(runs):
        offset = rng.randint(0, total_months - 1)

        year  = start_year + (start_month - 1 + offset) // 12
        month = (start_month - 1 + offset) % 12 + 1

        date = datetime.date(year, month, 1)

        params.append({
            "date": date.isoformat()
        })

    return params

def make_params_q11(seed, runs, sf):
    """
    TPC-H Query 11 parameter generator (规范级).

    Parameters:
    - nation   ∈ N_NAME
    - sf       = scale factor of the loaded TPC-H data set
    - fraction = 0.0001 / SF   (固定, 非随机)
    """
    if sf <= 0:
        raise ValueError("sf must be greater than zero")

    rng = random.Random(seed)

    nations = [
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
        "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
        "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
        "SAUDI ARABIA", "VIETNAM", "RUSSIA",
        "UNITED KINGDOM", "UNITED STATES"
    ]

    fraction = 0.0001 / sf   # 必须固定

    params = []
    for _ in range(runs):
        nation = rng.choice(nations)
        params.append({
            "nation": nation,
            "fraction": fraction
        })

    return params

def make_params_q12(seed, runs):
    """
    TPC-H Query 12 parameter generator (规范级).

    Parameters:
    - shipmode1 ∈ Modes
    - shipmode2 ∈ Modes, shipmode2 != shipmode1
    - date      = January 1st of a randomly selected year in [1993 .. 1997]
    """
    rng = random.Random(seed)

    shipmodes = [
        "REG AIR",
        "AIR",
        "RAIL",
        "SHIP",
        "TRUCK",
        "MAIL",
        "FOB",
    ]

    years = list(range(1993, 1998))  # [1993..1997]

    params = []

    for _ in range(runs):
        shipmode1, shipmode2 = rng.sample(shipmodes, 2)

        year = rng.choice(years)
        date = datetime.date(year, 1, 1)

        params.append({
            "shipmode1": shipmode1,
            "shipmode2": shipmode2,
            "date": date.isoformat()
        })

    return params

def make_params_q13(seed, runs):
    """
    TPC-H Query 13 parameter generator (format version).

    pattern = '%' || WORD1 || '%' || WORD2 || '%'
    """
    rng = random.Random(seed)

    word1_values = ["special", "pending", "unusual", "express"]
    word2_values = ["packages", "requests", "accounts", "deposits"]

    params = []

    for _ in range(runs):
        word1 = rng.choice(word1_values)
        word2 = rng.choice(word2_values)

        params.append({
            "pattern": f"%{word1}%{word2}%"
        })

    return params

def make_params_q14(seed, runs):
    """
    TPC-H Query 14 parameter generator (format version).

    DATE is the first day of a randomly selected month
    from a randomly selected year within [1993 .. 1997].
    """
    rng = random.Random(seed)

    years = list(range(1993, 1998))   # inclusive

    params = []

    for _ in range(runs):
        year = rng.choice(years)
        month = rng.randint(1, 12)

        date = datetime.date(year, month, 1)

        params.append({
            "date": date.isoformat()
        })

    return params

def make_params_q15(seed, runs):
    """
    TPC-H Query 15 parameter generator (format version).

    DATE is the first day of a randomly selected month
    between 1993-01 and 1997-10 (inclusive).

    Used in SQL as:
        l_shipdate >= DATE
        l_shipdate < DATE + INTERVAL '3 month'
    """
    rng = random.Random(seed)

    start_year, start_month = 1993, 1
    end_year, end_month     = 1997, 10

    total_months = (end_year - start_year) * 12 + (end_month - start_month) + 1

    params = []

    for _ in range(runs):
        offset = rng.randint(0, total_months - 1)

        year  = start_year + (start_month - 1 + offset) // 12
        month = (start_month - 1 + offset) % 12 + 1

        date = datetime.date(year, month, 1)

        params.append({
            "date": date.isoformat()
        })

    return params

def make_params_q16(seed, runs):
    rng = random.Random(seed)

    syllable1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    syllable2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
    syllable3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

    types_3 = [
        f"{a} {b} {c}"
        for a in syllable1
        for b in syllable2
        for c in syllable3
    ]

    params = []

    for _ in range(runs):
        # BRAND
        m = rng.randint(1, 5)
        n = rng.randint(1, 5)
        brand = f"Brand#{m}{n}"

        # TYPE prefix (前两段)
        full_type = rng.choice(types_3)
        type_prefix = " ".join(full_type.split()[:2])

        # SIZE: 8 distinct
        sizes = rng.sample(range(1, 51), 8)

        params.append({
            "brand": brand,
            "type_prefix": type_prefix,
            "sizes": sizes
        })

    return params

def make_params_q17(seed, runs):
    """
    TPC-H Query 17 parameter generator (format version).
    """
    rng = random.Random(seed)

    syllable1 = ["SM", "LG", "MED", "JUMBO", "WRAP"]
    syllable2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]

    containers_2 = [
        f"{a} {b}"
        for a in syllable1
        for b in syllable2
    ]

    params = []

    for _ in range(runs):
        # BRAND
        m = rng.randint(1, 5)
        n = rng.randint(1, 5)
        brand = f"Brand#{m}{n}"

        # CONTAINER
        container = rng.choice(containers_2)

        params.append({
            "brand": brand,
            "container": container
        })

    return params

def make_params_q18(seed, runs):
    """
    TPC-H Query 18 parameter generator (format version).

    Parameter:
    1. quantity ∈ [312 .. 315]
    """
    rng = random.Random(seed)

    params = []

    for _ in range(runs):
        quantity = rng.randint(312, 315)

        params.append({
            "quantity": quantity
        })

    return params

def make_params_q19(seed, runs):
    """
    TPC-H Query 19 parameter generator (format version).

    Parameters:
    - q1 ∈ [1..10]
    - q2 ∈ [10..20]
    - q3 ∈ [20..30]
    - brand1, brand2, brand3 = Brand#MN
    """
    rng = random.Random(seed)

    def rand_brand():
        return f"Brand#{rng.randint(1,5)}{rng.randint(1,5)}"

    params = []

    for _ in range(runs):
        params.append({
            "brand1": rand_brand(),
            "brand2": rand_brand(),
            "brand3": rand_brand(),
            "q1": rng.randint(1, 10),
            "q2": rng.randint(10, 20),
            "q3": rng.randint(20, 30),
        })

    return params

def make_params_q20(seed, runs):
    """
    TPC-H Query 20 parameter generator (format version).

    Parameters:
    - p_name : COLOR%
    - date   : YYYY-01-01, year ∈ [1993..1997]
    - nation : TPC-H nation
    """
    rng = random.Random(seed)

    COLORS = [
        "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
        "blanched", "blue", "blush", "brown", "burlywood", "chartreuse",
        "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
        "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
        "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
        "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
        "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
        "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
        "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
        "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
        "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
        "wheat", "white", "yellow"
    ]

    NATIONS = [
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
        "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
        "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
        "SAUDI ARABIA", "VIETNAM", "RUSSIA",
        "UNITED KINGDOM", "UNITED STATES"
    ]

    params = []

    for _ in range(runs):
        color = rng.choice(COLORS)
        year = rng.randint(1993, 1997)
        nation = rng.choice(NATIONS)

        params.append({
            "p_name": f"{color}%",
            "date": datetime.date(year, 1, 1).isoformat(),
            "nation": nation
        })

    return params

def make_params_q21(seed, runs):
    """
    TPC-H Query 21 parameter generator (format version).

    Parameter:
    - nation ∈ TPC-H nation list
    """
    rng = random.Random(seed)

    NATIONS = [
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
        "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
        "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
        "SAUDI ARABIA", "VIETNAM", "RUSSIA",
        "UNITED KINGDOM", "UNITED STATES"
    ]

    params = []

    for _ in range(runs):
        nation = rng.choice(NATIONS)
        params.append({
            "nation": nation
        })

    return params

def make_params_q22(seed, runs):
    """
    TPC-H Query 22 parameter generator (format version).

    I1..I7 are 7 distinct country codes.
    Passed to Cypher as a list of strings.
    """
    rng = random.Random(seed)

    COUNTRY_CODES = [str(i) for i in range(10, 35)]  # "10" .. "34"

    params = []

    for _ in range(runs):
        codes = rng.sample(COUNTRY_CODES, 7)

        params.append({
            "codes": codes
        })

    return params
