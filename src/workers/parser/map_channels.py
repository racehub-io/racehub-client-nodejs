# -*- coding: utf-8 -*-
"""
Created on Sat Aug 28 17:57:02 2021

@author: msansoni
"""

import pandas as pd

df = pd.read_csv('channel_list.csv')

f = open('createParquetSchema.js', 'w')

print("exports.createSchema = () => ({", file=f)
for index, row in df.iterrows():
    print("{output_name}: {{ type: '{output_type}', compression: 'GZIP' }},".format(output_name=row.output_name, output_type=row.output_type), file=f)
print("})", file=f)

f.close()

f = open('createMessage.js', 'w')

print("const { constants } = require('@racehub-io/f1-telemetry-client');", file=f)
print("const { DRIVERS, FORMULAS, NATIONALITIES, SESSION_TYPES, TEAMS, TRACKS } = constants;", file=f)
print("exports.createMessage = (motionData, sessionData, lapData, participantData, setupData, telemetryData, statusData, damageData) => {", file=f)
print("return {", file=f)
for index, row in df.iterrows():
    if row.include:
        print("{output_name}: {input_path},".format(output_name=row.output_name, input_path=row.input_path), file=f)
print("})", file=f)

f.close()
