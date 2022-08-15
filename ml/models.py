import pandas as pd

from voter_history.models import VoterHistory
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from numpy import unique
from keras.layers import Input
from keras.layers import Dense
from keras.layers import Embedding
from keras.layers.merging import concatenate
from keras.utils import plot_model
from keras.models import Model
import numpy as np


class PrimaryDataSet:
    def __init__(self, voters, election_date):
        self.election_date = election_date
        self.voters = voters
        self.dataset_ = None
        self.categories = [0, 1]
        self.ordinals = [2, 3, 4]
        self.history = None

    @property
    def dataset(self):
        if self.dataset_ is not None:
            dataset = self.dataset_.values
            X = dataset[:, :-1]
            y = dataset[:, -1]
            y = y.reshape((len(y), 1))
            return X, y
        voter_ids = [v.voter_id for v in self.voters]
        precincts = set([v.precinct for v in self.voters])
        precincts = pd.DataFrame.from_records(
            [{
                'precinct': p,
                'center': p.precinct_map.center
            } for p in precincts]
        )
        precincts = precincts.assign(lon=precincts.center.apply(lambda c: c.x), lat=precincts.center.apply(lambda c: c.y)).drop(columns='center')

        vh = VoterHistory.objects.get_for(self.election_date, voter_ids)[['voter_id', 'party']]
        df = pd.DataFrame.from_records(
            [{
                'voter_id': v.voter_id,
                'precinct': v.precinct,
                'race_id': v.race_id,
                'gender': v.gender,
                'year_of_birth': max(1943, v.year_of_birth)
            } for v in self.voters]
        )
        df = df.merge(precincts, on='precinct', how='inner')
        df = df.merge(vh, on='voter_id', how='inner')
        df = df[(df.party == 'R') | (df.party == 'D')]
        if len(df.index) == 0:
            raise ValueError(f'Election on {self.election_date.strptime("%Y-%m-%d")} is not a primary!')
        df = df.assign(year_of_birth=df.year_of_birth.apply(lambda x: max(1942, x)))
        self.dataset_ = df[['race_id', 'gender', 'lon', 'lat', 'year_of_birth', 'party']]
        return self.dataset

    def age_decorator(self, df):
        year = self.election_date.year()
        df = df.assign(age=year - df.year_of_birth)
        df = df.assign(age=df.age.apply(lambda x: max(min(x, 80), 18)))
        return df

    # prepare input data
    def prepare_inputs(self, X_train, X_test):
        X_train_enc, X_test_enc = list(), list()
        # label encode each race and gender
        # Use embeddings for the categorical
        # variables.
        for i in self.categories:
            le = LabelEncoder()
            le.fit(X_train[:, i])
            # encode
            train_enc = le.transform(X_train[:, i])
            test_enc = le.transform(X_test[:, i])
            # store
            X_train_enc.append(train_enc)
            X_test_enc.append(test_enc)
        # Scale longitude, latitude, and year of birth
        for i in self.ordinals:
            ss = StandardScaler().fit(X_train[:, i].reshape(-1, 1))
            train_ss = ss.transform(X_train[:, i].reshape(-1, 1))
            test_ss = ss.transform(X_test[:, i].reshape(-1, 1))
            X_train_enc.append(train_ss)
            X_test_enc.append(test_ss)
        return X_train_enc, X_test_enc

    # prepare target
    def prepare_targets(self, y_train, y_test):
        le = LabelEncoder()
        le.fit(y_train[:, 0])
        y_train_enc = le.transform(y_train[:, 0])
        y_test_enc = le.transform(y_test[:, 0])
        return y_train_enc, y_test_enc

    def dosomething(self):
        X, y = self.dataset
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1)
        # prepare input data
        X_train_enc, X_test_enc = self.prepare_inputs(X_train, X_test)
        # prepare output data
        y_train_enc, y_test_enc = self.prepare_targets(y_train, y_test)
        # make output 3d
        y_train_enc = y_train_enc.reshape((len(y_train_enc), 1, 1))
        y_test_enc = y_test_enc.reshape((len(y_test_enc), 1, 1))
        # prepare each input head
        in_layers = list()
        em_layers = list()
        for i in self.categories:
            # calculate the number of unique inputs
            n_labels = len(unique(X_train_enc[i]))
            embedding_size = int(min(np.ceil(n_labels / 2), 50))
            # define input layer
            in_layer = Input(shape=(1,))
            # define embedding layer
            em_layer = Embedding(n_labels, embedding_size)(in_layer)
            # store layers
            in_layers.append(in_layer)
            em_layers.append(em_layer)
        # concat all embeddings
        ordinal_layers = [Input(shape=(1, 1)), Input(shape=(1, 1)), Input(shape=(1, 1))]
        in_layers.extend(ordinal_layers)
        em_layers.extend(ordinal_layers)
        model = concatenate(em_layers)
        model = Dense(200, activation="relu")(model)
        model = Dense(50, activation="relu")(model)
        output = Dense(1, activation='sigmoid')(model)
        model = Model(inputs=in_layers, outputs=output)
        # compile the keras model
        model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
        # plot graph
        plot_model(model, show_shapes=True, to_file='embeddings.png')
        # fit the keras model on the dataset
        self.history = model.fit(X_train_enc, y_train_enc, validation_split=0.10, epochs=100, batch_size=64, verbose=2)
        # evaluate the keras model
        _, accuracy = model.evaluate(X_test_enc, y_test_enc, verbose=0)
        print('Accuracy: %.2f' % (accuracy * 100))
