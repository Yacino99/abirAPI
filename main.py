from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/air_quality'

# Créer un moteur SQLAlchemy
engine = create_engine('mysql://root:root@localhost')

# Créer une base de données
engine.execute("CREATE DATABASE IF NOT EXISTS air_quality")

# Sélectionner la nouvelle base de données
engine.execute("USE air_quality")

db = SQLAlchemy(app)


class Organisme(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {'id': self.id, 'name': self.name}

class Zas(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    organisme_id = db.Column(db.Integer, db.ForeignKey('organisme.id'), nullable=False)
    name = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {'id': self.id, 'organisme_id': self.organisme_id, 'name': self.name}

class Station(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    zas_id = db.Column(db.Integer, db.ForeignKey('zas.id'), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    type_d_implantation = db.Column(db.String(255))

    def to_dict(self):
        return {
            'id': self.id,
            'zas_id': self.zas_id,
            'name': self.name,
            'type_d_implantation': self.type_d_implantation
        }

class Polluant(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {'id': self.id, 'name': self.name}

class Mesure(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.Integer, db.ForeignKey('station.id'), nullable=False)
    polluant_id = db.Column(db.Integer, db.ForeignKey('polluant.id'), nullable=False)
    value = db.Column(db.Float)
    valeur_brute = db.Column(db.Float)
    unité_de_mesure = db.Column(db.String(255))
    taux_de_saisie = db.Column(db.Float)
    couverture_temporelle = db.Column(db.Float)
    couverture_de_données = db.Column(db.Float)
    code_qualité = db.Column(db.String(1))
    validité = db.Column(db.Integer)
    measurement_date = db.Column(db.DateTime)

'''
@app.route('/organismes', methods=['GET'])
def get_organismes():
    organismes = Organisme.query.all()
    return jsonify([organisme.to_dict() for organisme in organismes])

@app.route('/organismes/<id>', methods=['GET'])
def get_organisme(id):
    organisme = Organisme.query.get(id)
    return jsonify(organisme.to_dict())

@app.route('/zas', methods=['GET'])
def get_zas():
    zas = Zas.query.all()
    return jsonify([zas.to_dict() for zas in zas])

@app.route('/zas/<id>', methods=['GET'])
def get_zas(id):
    zas = Zas.query.get(id)
    return jsonify(zas.to_dict())

@app.route('/stations', methods=['GET'])
def get_stations():
    stations = Station.query.all()
    return jsonify([station.to_dict() for station in stations])

@app.route('/stations/<id>', methods=['GET'])
def get_station(id):
    station = Station.query.get(id)
    return jsonify(station.to_dict())

@app.route('/polluants', methods=['GET'])
def get_polluants():
    polluants = Polluant.query.all()
    return jsonify([polluant.to_dict() for polluant in polluants])

@app.route('/polluants/<id>', methods=['GET'])
def get_polluant(id):
    polluant = Polluant.query.get(id)
    return jsonify(polluant.to_dict())

@app.route('/mesures', methods=['GET'])
def get_mesures():
    mesures = Mesure.query.all()
    return jsonify([mesure.to_dict() for mesure in mesures])

@app.route('/mesures/<id>', methods=['GET'])
def get_mesure(id):
    mesure = Mesure.query.get(id)
    return jsonify(mesure.to_dict())

@app.route('/mesures', methods=['POST'])
def add_mesure():
    new_mesure = Mesure(
        station_id=request.json['station_id'],
        polluant_id=request.json['polluant_id'],
        value=request.json['value'],
        measurement_date=request.json['measurement_date']
    )
    db.session.add(new_mesure)
    db.session.commit()
    return jsonify(new_mesure.to_dict()), 201
'''

@app.route('/stations', methods=['GET'])
def get_stations():
    stations = Station.query.all()
    return jsonify([station.to_dict() for station in stations])

@app.route('/pollutants', methods=['GET'])
def get_pollutants():
    pollutants = Polluant.query.all()
    return jsonify([pollutant.to_dict() for pollutant in pollutants])

@app.route('/regional_centers', methods=['GET'])
def get_regional_centers():
    regional_centers = Organisme.query.all()
    return jsonify([regional_center.to_dict() for regional_center in regional_centers])

#Récupération des Mesures pour une Station Spécifique :
#Endpoint : /measurements?pollutant_id={pollutant_id}

@app.route('/measurements', methods=['GET'])
def get_measurements():
    station_id = request.args.get('station_id')

    if station_id is not None:
        measurements = Mesure.query.filter_by(station_id=station_id).all()
        return jsonify([measurement.to_dict() for measurement in measurements])
    else:
        return jsonify({"error": "No station_id provided"}), 400

#Récupération des Mesures pour un Polluant Spécifique :
@app.route('/measurements', methods=['GET'])
def get_measurements():
    pollutant_id = request.args.get('pollutant_id')

    if pollutant_id is not None:
        measurements = Mesure.query.filter_by(polluant_id=pollutant_id).all()
        return jsonify([measurement.to_dict() for measurement in measurements])
    else:
        return jsonify({"error": "No pollutant_id provided"}), 400

#Récupération des Mesures pour une Période Spécifique :
#Endpoint : /measurements?station_id={station_id}&start_date={yyyy-mm-dd}&end_date={yyyy-mm-dd}
@app.route('/measurements', methods=['GET'])
def get_measurements():
    station_id = request.args.get('station_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if station_id is not None and start_date is not None and end_date is not None:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        measurements = Mesure.query.filter(Mesure.station_id == station_id, Mesure.measurement_date.between(start_date, end_date)).all()
        return jsonify([measurement.to_dict() for measurement in measurements])
    else:
        return jsonify({"error": "station_id, start_date, and end_date must be provided"}), 400


#Récupération de la Moyenne Journalière pour un Polluant Spécifique :
#Endpoint : /daily_average?pollutant_id={pollutant_id}&date={yyyy-mm-dd}

@app.route('/daily_average', methods=['GET'])
def get_daily_average():
    pollutant_id = request.args.get('pollutant_id')
    date = request.args.get('date')

    if pollutant_id is not None and date is not None:
        date = datetime.strptime(date, '%Y-%m-%d')
        measurements = Mesure.query.filter_by(pollutant_id=pollutant_id, measurement_date=date).all()
        daily_average = sum(measurement.value for measurement in measurements) / len(measurements)
        return jsonify({'daily_average': daily_average})
    else:
        return jsonify({"error": "pollutant_id and date must be provided"}), 400

#Récupération de la Moyenne Journalière pour tous les Polluants sur une Station :
#Endpoint : /daily_average?station_id={station_id}&date={yyyy-mm-dd}
@app.route('/daily_average', methods=['GET'])
def get_daily_average():
    station_id = request.args.get('station_id')
    date = request.args.get('date')

    if station_id is not None and date is not None:
        date = datetime.strptime(date, '%Y-%m-%d')
        pollutants = Polluant.query.all()
        daily_averages = {}
        for pollutant in pollutants:
            measurements = Mesure.query.filter_by(station_id=station_id, pollutant_id=pollutant.id, measurement_date=date).all()
            if measurements:
                daily_average = sum(measurement.value for measurement in measurements) / len(measurements)
                daily_averages[pollutant.name] = daily_average
        return jsonify(daily_averages)
    else:
        return jsonify({"error": "station_id and date must be provided"}), 400

#Récupération du Maximum et Minimum pour un Polluant Spécifique sur une Période :
#Endpoint : /range?pollutant_id={pollutant_id}&start_date={yyyy-mm-dd}&end_date={yyyy-mm-dd}
@app.route('/range', methods=['GET'])
def get_range():
    pollutant_id = request.args.get('pollutant_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if pollutant_id is not None and start_date is not None and end_date is not None:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        measurements = Mesure.query.filter(Mesure.polluant_id == pollutant_id, Mesure.measurement_date.between(start_date, end_date)).all()
        min_value = min(measurement.value for measurement in measurements)
        max_value = max(measurement.value for measurement in measurements)
        return jsonify({'min_value': min_value, 'max_value': max_value})
    else:
        return jsonify({"error": "pollutant_id, start_date, and end_date must be provided"}), 400

#Récupération des Mesures pour une Station, un Polluant et une Date Précise :
#Endpoint : /measurements?station_id={station_id}&pollutant_id={pollutant_id}&date={yyyy-mm-dd
@app.route('/measurements', methods=['GET'])
def get_measurements():
    station_id = request.args.get('station_id')
    pollutant_id = request.args.get('pollutant_id')
    date = request.args.get('date')

    if station_id is not None and pollutant_id is not None and date is not None:
        date = datetime.strptime(date, '%Y-%m-%d')
        measurements = Mesure.query.filter(Mesure.station_id == station_id, Mesure.polluant_id == pollutant_id, func.date(Mesure.measurement_date) == date).all()
        return jsonify([measurement.to_dict() for measurement in measurements])
    else:
        return jsonify({"error": "station_id, pollutant_id, and date must be provided"}), 400


def load_data_to_db(csv_file):
    # Lire les données du fichier CSV
    df = pd.read_csv(csv_file)

    # Créer un moteur SQLAlchemy
    engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

    # Charger les données dans chaque table
    df[['id', 'name']].drop_duplicates().to_sql('Organisme', engine, if_exists='append', index=False)
    df[['id', 'organisme_id', 'name']].drop_duplicates().to_sql('Zas', engine, if_exists='append', index=False)
    df[['id', 'zas_id', 'name', 'type_d_implantation']].drop_duplicates().to_sql('Station', engine, if_exists='append', index=False)
    df[['id', 'name']].drop_duplicates().to_sql('Polluant', engine, if_exists='append', index=False)
    df[['id', 'station_id', 'polluant_id', 'value', 'valeur_brute', 'unité_de_mesure', 'taux_de_saisie', 'couverture_temporelle', 'couverture_de_données', 'code_qualité', 'validité', 'measurement_date']].to_sql('Mesure', engine, if_exists='append', index=False)

if __name__ == '__main__':
    db.create_all()
    load_data_to_db('FR_E2_2021-01-01.csv')
    app.run(debug=True)
