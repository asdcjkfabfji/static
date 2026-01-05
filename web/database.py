import sqlite3

def get_db_connection():
    """Ma'lumotlar bazasiga ulanish yaratish"""
    conn = sqlite3.connect('students.db')
    # SQL natijalarini lug'at (dictionary) ko'rinishida olish uchun
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Barcha kerakli jadvallarni yaratish va boshlang'ich ma'lumotlarni kiritish"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Talabalar jadvali (HEMIS ma'lumotlari uchun)
    cursor.execute('''CREATE TABLE IF NOT EXISTS students (
        student_id TEXT PRIMARY KEY, 
        full_name TEXT, 
        gender TEXT, 
        level TEXT, 
        group_name TEXT, 
        department TEXT, 
        education_form TEXT, 
        payment_form TEXT, 
        year TEXT, 
        semester TEXT, 
        gpa TEXT
    )''')

    # 2. Foydalanuvchilar jadvali (Login tizimi uchun)
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, 
        password TEXT, 
        role TEXT
    )''')

    # 3. Tizim sozlamalari jadvali (Tugmalarni yoqish/o'chirish uchun)
    cursor.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, 
        value INTEGER
    )''')

    # --- Boshlang'ich ma'lumotlarni kiritish ---

    # Standart admin va foydalanuvchini qo'shish (agar jadval bo'sh bo'lsa)
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        # Standart login: admin, parol: 123
        cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", 
                       ('admin', '123', 'admin'))
        # Standart login: user, parol: 111
        cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", 
                       ('user', '111', 'viewer'))
        print("Standart foydalanuvchilar yaratildi.")

    # Standart sozlamalarni kiritish (1 - yoqilgan, 0 - o'chirilgan)
    cursor.execute("SELECT COUNT(*) FROM settings")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('sync_enabled', 1))
        cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('excel_enabled', 1))
        print("Tizim sozlamalari (tugmalar) faollashtirildi.")

    conn.commit()
    conn.close()

def save_to_db(students_json):
    """HEMIS API dan kelgan JSON ma'lumotlarini bazaga saqlash"""
    conn = get_db_connection()
    cursor = conn.cursor()
    data = []
    
    for s in students_json:
        data.append((
            str(s.get('student_id_number', '')), 
            s.get('full_name', ''),
            s.get('gender', {}).get('name', '') if s.get('gender') else '',
            s.get('level', {}).get('name', '') if s.get('level') else '',
            s.get('group', {}).get('name', '') if s.get('group') else '',
            s.get('department', {}).get('name', '') if s.get('department') else '',
            s.get('educationForm', {}).get('name', '') if s.get('educationForm') else '',
            s.get('paymentForm', {}).get('name', '') if s.get('paymentForm') else '',
            s.get('educationYear', {}).get('name', '') if s.get('educationYear') else '',
            s.get('semester', {}).get('name', '') if s.get('semester') else '',
            str(s.get('gpa', '0.0'))
        ))
    
    # Ma'lumotlarni kiritish (agar student_id mavjud bo'lsa, ustiga yozmaydi)
    cursor.executemany(
        "INSERT OR IGNORE INTO students VALUES (?,?,?,?,?,?,?,?,?,?,?)", 
        data
    )
    
    conn.commit()
    conn.close()

def get_settings():
    """Tizim sozlamalarini lug'at ko'rinishida qaytarish"""
    conn = get_db_connection()
    rows = conn.execute("SELECT * FROM settings").fetchall()
    conn.close()
    return {row['key']: row['value'] for row in rows}

def toggle_setting(key):
    """Tizim sozlamasini (on/off) o'zgartirish"""
    conn = get_db_connection()
    current = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if current:
        new_val = 0 if current['value'] == 1 else 1
        conn.execute("UPDATE settings SET value = ? WHERE key = ?", (new_val, key))
        conn.commit()
    conn.close()