from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/air_quality'
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

    def to_dict(self):
        return {'id': self.id, 'zas_id': self.zas_id, 'name': self.name}

class Polluant(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {'id': self.id, 'name': self.name}

class Mesure(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.Integer, db.ForeignKey('station.id'), nullable=False)
    polluant_id = db.Column(db.Integer, db.ForeignKey('polluant.id'), nullable=False)
    value = db.Column(db.Float, nullable=False)
    measurement_date = db.Column(db.DateTime, nullable=False)

    def to_dict(self):
        return {'id': self.id, 'station_id': self.station_id, 'polluant_id': self.polluant_id, 'value': self.value, 'measurement_date': self.measurement_date}

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

if __name__ == '__main__':
    app.run(debug=True)
