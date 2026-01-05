import asyncio
import pandas as pd
from flask import Flask, render_template, request, Response, send_file, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from database import init_db, get_db_connection, get_settings, toggle_setting
from scraper import get_all_data_stream
from reports import generate_excel

app = Flask(__name__)
app.secret_key = "hemis_secure_2026_key"

# --- LOGIN SOZLAMALARI ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, id, role):
        self.id = id
        self.role = role

@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    user_data = conn.execute("SELECT * FROM users WHERE username = ?", (user_id,)).fetchone()
    conn.close()
    if user_data:
        return User(user_data['username'], user_data['role'])
    return None

# --- ADMIN FUNKSIYALARI ---
@app.route('/admin/users', methods=['GET', 'POST'])
@login_required
def manage_users():
    if current_user.role != 'admin':
        return redirect(url_for('index'))
    
    conn = get_db_connection()
    if request.method == 'POST':
        u = request.form.get('username')
        p = request.form.get('password')
        r = request.form.get('role')
        try:
            conn.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", (u, p, r))
            conn.commit()
            flash(f"Foydalanuvchi {u} muvaffaqiyatli qo'shildi", "success")
        except:
            flash("Xato: Bunday foydalanuvchi allaqachon mavjud", "danger")
    
    users = conn.execute("SELECT * FROM users").fetchall()
    conn.close()
    return render_template('users.html', users=users)

@app.route('/admin/delete_user/<username>')
@login_required
def delete_user(username):
    if current_user.role == 'admin' and username != 'admin':
        conn = get_db_connection()
        conn.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        conn.close()
    return redirect(url_for('manage_users'))

@app.route('/admin/toggle/<key>')
@login_required
def toggle_btn(key):
    if current_user.role == 'admin':
        toggle_setting(key)
    return redirect(url_for('index'))

# --- ASOSIY STATISTIKA SAHIFASI ---
@app.route('/')
@login_required
def index():
    settings = get_settings()
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM students", conn)
    except:
        df = pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        return render_template('index.html', report_data={}, years=[], selected_year="", 
                               all_courses=[], global_stats={'total':0}, settings=settings)

    all_years = sorted(df['year'].unique(), reverse=True)
    selected_year = request.args.get('year', all_years[0])
    df_f = df[df['year'] == selected_year]

    # Global Vidjetlar uchun statistika
    stats = {
        'total': len(df_f),
        'male': (df_f['gender'] == 'Erkak').sum(),
        'female': (df_f['gender'] == 'Ayol').sum(),
        'grant': df_f['payment_form'].str.contains('grant', case=False, na=False).sum(),
        'contract': (~df_f['payment_form'].str.contains('grant', case=False, na=False)).sum()
    }

    all_courses = sorted(df_f['level'].unique())
    report_data = {}
    
    # Fakultetlar bo'yicha guruhlash
    for fak in df_f['department'].unique():
        report_data[fak] = {}
        fak_df = df_f[df_f['department'] == fak]
        for shakl in fak_df['education_form'].unique():
            shakl_df = fak_df[fak_df['education_form'] == shakl]
            report_data[fak][shakl] = {}
            for kurs in all_courses:
                k_df = shakl_df[shakl_df['level'] == kurs]
                if not k_df.empty:
                    # Guruhlar bo'yicha hisoblash
                    report_data[fak][shakl][kurs] = k_df.groupby('group_name').agg(
                        jami=('student_id', 'count'),
                        g_jami=('payment_form', lambda x: x.str.contains('grant', case=False).sum()),
                        k_jami=('payment_form', lambda x: (~x.str.contains('grant', case=False)).sum()),
                        u_q=('gender', lambda x: (x == 'Ayol').sum()),
                        u_o=('gender', lambda x: (x == 'Erkak').sum())
                    ).reset_index().to_dict('records')

    return render_template('index.html', report_data=report_data, years=all_years, 
                           selected_year=selected_year, global_stats=stats, 
                           all_courses=all_courses, settings=settings)

# --- GURUH TAFSILOTLARI ---
@app.route('/group/<group_name>')
@login_required
def group_detail(group_name):
    p_type = request.args.get('type')
    gender = request.args.get('gender')
    
    conn = get_db_connection()
    query = "SELECT * FROM students WHERE group_name = ?"
    params = [group_name]

    if p_type == 'grant':
        query += " AND payment_form LIKE '%grant%'"
    elif p_type == 'contract':
        query += " AND payment_form NOT LIKE '%grant%'"
    
    if gender:
        query += " AND gender = ?"
        params.append(gender)

    students = conn.execute(query, params).fetchall()
    conn.close()
    return render_template('group_detail.html', students=students, group_name=group_name)

# --- AVTORIZATSIYA ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        u = request.form.get('username')
        p = request.form.get('password')
        conn = get_db_connection()
        user = conn.execute("SELECT * FROM users WHERE username=? AND password=?", (u, p)).fetchone()
        conn.close()
        if user:
            login_user(User(user['username'], user['role']))
            return redirect(url_for('index'))
        flash('Login yoki parol xato!', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

# --- EKSPORT VA SINXRONIZATSIYA ---
@app.route('/sync-stream')
@login_required
def sync_stream():
    settings = get_settings()
    if current_user.role != 'admin' and not settings.get('sync_enabled'):
        return "Kirish taqiqlangan", 403
        
    def generate():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        gen = get_all_data_stream()
        try:
            while True:
                message = loop.run_until_complete(gen.__anext__())
                yield f"data: {message}\n\n"
        except StopAsyncIteration:
            yield "data: Sinxronizatsiya yakunlandi!\n\n"
        finally:
            loop.close()
    return Response(generate(), mimetype='text/event-stream')

@app.route('/export')
@login_required
def export():
    settings = get_settings()
    if current_user.role != 'admin' and not settings.get('excel_enabled'):
        return "Kirish taqiqlangan", 403
        
    year = request.args.get('year')
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM students WHERE year = ?", conn, params=(year,))
    conn.close()
    
    filename = f"Talabalar_Statistikasi_{year}.xlsx"
    generate_excel(df, filename, year)
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    init_db() # Ma'lumotlar bazasini tekshirish/yaratish
    app.run(debug=True, port=5000)