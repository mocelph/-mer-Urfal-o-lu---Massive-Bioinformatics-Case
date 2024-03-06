# API Sanal ortam oluşturularak çalıştırılmıştır.
# python3 -m venv .venv
# . .venv/bin/activate

from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

#Bağlantı parametrelerini değişkenlerde tutmak güvenliği arttırabilir. Yalın olarak kod içerisinde bulunmasını engellemiş oluruz.
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "1998Moc1998"
DB_PORT = "5432"
DB_HOST = "localhost"

# PostgreSQL veritabanına bağlanma fonksiyonu
def connect_to_database():
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            host=DB_HOST
        )
        return connection
    except psycopg2.Error as e:
        print("Error while connecting to PostgreSQL:", e)
        return None

# SQL sorgusu oluşturma fonksiyonu
def build_sql_query(filters, ordering, page, per_page):
    sql_query = "SELECT * FROM report_output WHERE 1=1"
    params = []

    # Filtrelerin SQL sorgusuna eklenmesi
    for column, value in filters.items():
        sql_query += f" AND {column} = %s"
        params.append(value)

    # Sıralama eklenmesi
    if ordering:
        sql_query += " ORDER BY "
        for order in ordering:
            column = list(order.keys())[0]
            direction = order[column]
            sql_query += f"{column} {direction}, "
        sql_query = sql_query.rstrip(', ')

    # Sayfalama ve limit eklenmesi
    offset = (page - 1) * per_page
    sql_query += f" OFFSET {offset} LIMIT {per_page}"

    return sql_query, params

# Filtreleri doğrulama fonksiyonu
def validate_filters(filters):
    for column, value in filters.items():
        if column in ["main.uploaded_variation", "main.existing_variation", "main.symbol"]:
            if not isinstance(value, list) or len(value) != 3 or not all(isinstance(item, str) for item in value):
                return False
        elif column in ["main.af_vcf", "main.dp", "details2.dann_score"]:
            if not (isinstance(value, (int, float)) or (isinstance(value, list) and len(value) in [1, 2] and all(isinstance(item, (int, float)) for item in value))):
                return False
            if isinstance(value, list) and len(value) == 1 and isinstance(value[0], str):
                return False
        elif column in ["links.mondo", "links.pheno pubmed", "details2.provean"]:
            if not isinstance(value, str):
                return False
    return True

# Flask endpoint'i
@app.route('/assignment/query', methods=['POST'])
def get_report_output():
    
    # PostgreSQL'e bağlan
    connection = connect_to_database()

    if connection is not None:
        try:
            cursor = connection.cursor()

            # URL'den gelen parametreleri al
            page = request.args.get("page", default=1, type=int)
            per_page = request.args.get("per_page", default=20, type=int)  # Örnek olarak 20 sayfa başı 20 veri, performansı arttırmak için sayı düşük tutulabilir.

            # İsteğin JSON içeriğini al
            content_type = request.headers.get('Content-Type')
            if content_type != 'application/json':
                return jsonify({'error': 'Unsupported Media Type. Content-Type must be application/json.'}), 415

            data = request.get_json()

            if not data:
                return jsonify({'error': 'Invalid JSON format in the request.'}), 400

            # JSON içeriğinden gerekli bilgileri çek
            filters = data.get("filters", {})
            ordering = data.get("ordering", {})

            # Filtreleri kontrol et
            if not validate_filters(filters):
                return jsonify({'error': 'Invalid filter format. Please check the filter rules.'}), 400

            # SQL sorgusunu oluştur
            sql_query, params = build_sql_query(filters, ordering, page, per_page)

            # Parametreleri kullanarak sorguyu çalıştır
            cursor.execute(sql_query, params)

            # Sorgudan gelen veriyi işle
            columns = [desc[0] for desc in cursor.description]
            report_output = [dict(zip(columns, row)) for row in cursor.fetchall()]

            # Veritabanındaki toplam kayıt sayısını al
            cursor.execute("SELECT COUNT(*) FROM report_output")
            count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return jsonify({'report_output': report_output, 'page': page, 'page_size': per_page, 'count': count}), 200
        except psycopg2.Error as e:
            print("Error while fetching data from PostgreSQL:", e)
            return jsonify({'error': f'Error while fetching data from PostgreSQL: {str(e)}'}), 500
        except Exception as e:
            print("Error:", e)
            return jsonify({'error': f'Internal Server Error: {str(e)}'}), 500
    else:
        return jsonify({'error': 'Failed to connect to the database'}), 500

if __name__ == '__main__':
    app.run(debug=True)
