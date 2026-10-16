/*
 * parser.js
 * This worker parses the raw data from the .rhc files and writes the data to parquet files.
 */

const {
  F1TelemetryClient,
  constants,
} = require('@racehub-io/f1-telemetry-client');
const path = require('path');
const { customAlphabet } = require('nanoid');
const { alphanumeric } = require('nanoid-dictionary');
const nanoid = customAlphabet(alphanumeric, 20);
const { isMainThread } = require('worker_threads');

const fs = require('fs');
const zlib = require('zlib');
const { parentPort, workerData } = require('worker_threads');

const { ParquetWriter, ParquetSchema } = require('parquets');

const { getPacketSize, parseBufferMessage } = F1TelemetryClient;
const { DRIVERS, FORMULAS, NATIONALITIES, PACKETS, SESSION_TYPES, TEAMS, TRACKS, WEATHER } =
  constants;

const os = require('os');

const createSchema = () => ({
  frameId: { type: 'UINT_32', compression: 'GZIP' },
  sessionTime: { type: 'FLOAT', compression: 'GZIP' },
  sessionId: { type: 'UTF8', compression: 'GZIP' },
  uniqueSessionId: { type: 'UTF8', compression: 'GZIP' },
  playerCarIndex: { type: 'UINT_8', compression: 'GZIP' },
  carIndex: { type: 'UINT_8', compression: 'GZIP' },
  world_x: { type: 'FLOAT', compression: 'GZIP' },
  world_y: { type: 'FLOAT', compression: 'GZIP' },
  world_z: { type: 'FLOAT', compression: 'GZIP' },
  g_lateral: { type: 'FLOAT', compression: 'GZIP' },
  g_longitudinal: { type: 'FLOAT', compression: 'GZIP' },
  g_vertical: { type: 'FLOAT', compression: 'GZIP' },
  yaw: { type: 'FLOAT', compression: 'GZIP' },
  pitch: { type: 'FLOAT', compression: 'GZIP' },
  roll: { type: 'FLOAT', compression: 'GZIP' },
  wheel_speed_FL: { type: 'FLOAT', compression: 'GZIP' },
  wheel_speed_FR: { type: 'FLOAT', compression: 'GZIP' },
  wheel_speed_RL: { type: 'FLOAT', compression: 'GZIP' },
  wheel_speed_RR: { type: 'FLOAT', compression: 'GZIP' },
  wheel_slip_FL: { type: 'FLOAT', compression: 'GZIP' },
  wheel_slip_FR: { type: 'FLOAT', compression: 'GZIP' },
  wheel_slip_RL: { type: 'FLOAT', compression: 'GZIP' },
  wheel_slip_RR: { type: 'FLOAT', compression: 'GZIP' },
  track_name: { type: 'UTF8', compression: 'GZIP' },
  track_length: { type: 'UINT_16', compression: 'GZIP' },
  session_type: { type: 'UTF8', compression: 'GZIP' },
  total_laps: { type: 'UINT_8', compression: 'GZIP' },
  formula: { type: 'UTF8', compression: 'GZIP' },
  weather: { type: 'UTF8', compression: 'GZIP' },
  track_temperature: { type: 'INT_8', compression: 'GZIP' },
  air_temperature: { type: 'INT_8', compression: 'GZIP' },
  session_time_remaining: { type: 'UINT_16', compression: 'GZIP' },
  safety_car_status: { type: 'UINT_8', compression: 'GZIP' },
  ai_difficulty: { type: 'UINT_8', compression: 'GZIP' },
  pitstop_rejoin_position: { type: 'UINT_8', compression: 'GZIP' },
  steering_assist: { type: 'UINT_8', compression: 'GZIP' },
  braking_assist: { type: 'UINT_8', compression: 'GZIP' },
  gearbox_assist: { type: 'UINT_8', compression: 'GZIP' },
  pit_assist: { type: 'UINT_8', compression: 'GZIP' },
  pit_release_assist: { type: 'UINT_8', compression: 'GZIP' },
  ers_assist: { type: 'UINT_8', compression: 'GZIP' },
  drs_assist: { type: 'UINT_8', compression: 'GZIP' },
  racing_line: { type: 'UINT_8', compression: 'GZIP' },
  racing_line_type: { type: 'UINT_8', compression: 'GZIP' },
  last_lap_time: { type: 'UINT_32', compression: 'GZIP' },
  lap_time: { type: 'UINT_32', compression: 'GZIP' },
  sector_1_time: { type: 'UINT_16', compression: 'GZIP' },
  sector_2_time: { type: 'UINT_16', compression: 'GZIP' },
  lap_distance: { type: 'FLOAT', compression: 'GZIP' },
  total_distance: { type: 'FLOAT', compression: 'GZIP' },
  safety_car_delta: { type: 'FLOAT', compression: 'GZIP' },
  car_position: { type: 'UINT_8', compression: 'GZIP' },
  lap_number: { type: 'UINT_8', compression: 'GZIP' },
  pit_status: { type: 'UINT_8', compression: 'GZIP' },
  sector: { type: 'UINT_8', compression: 'GZIP' },
  lap_invalid: { type: 'UINT_8', compression: 'GZIP' },
  penalties: { type: 'UINT_8', compression: 'GZIP' },
  warnings: { type: 'UINT_8', compression: 'GZIP' },
  grid_position: { type: 'UINT_8', compression: 'GZIP' },
  driver_status: { type: 'UINT_8', compression: 'GZIP' },
  result_status: { type: 'UINT_8', compression: 'GZIP' },
  participant_isAI: { type: 'UINT_8', compression: 'GZIP' },
  participant_network_ID: { type: 'UINT_8', compression: 'GZIP' },
  participant_first_name: { type: 'UTF8', compression: 'GZIP' },
  participant_last_name: { type: 'UTF8', compression: 'GZIP' },
  participant_abbreviation: { type: 'UTF8', compression: 'GZIP' },
  participant_nationality: { type: 'UTF8', compression: 'GZIP' },
  participant_name: { type: 'UTF8', compression: 'GZIP' },
  participant_team_name: { type: 'UTF8', compression: 'GZIP' },
  participant_race_number: { type: 'UINT_8', compression: 'GZIP' },
  participant_udp_visibility: { type: 'UINT_8', compression: 'GZIP' },
  setup_front_wing: { type: 'UINT_8', compression: 'GZIP' },
  setup_rear_wing: { type: 'UINT_8', compression: 'GZIP' },
  setup_diff_on_throttle: { type: 'UINT_8', compression: 'GZIP' },
  setup_diff_off_throttle: { type: 'UINT_8', compression: 'GZIP' },
  setup_front_camber: { type: 'FLOAT', compression: 'GZIP' },
  setup_rear_camber: { type: 'FLOAT', compression: 'GZIP' },
  setup_front_toe: { type: 'FLOAT', compression: 'GZIP' },
  setup_rear_toe: { type: 'FLOAT', compression: 'GZIP' },
  setup_front_suspension: { type: 'UINT_8', compression: 'GZIP' },
  setup_rear_suspension: { type: 'UINT_8', compression: 'GZIP' },
  setup_front_anti_roll_bar: { type: 'UINT_8', compression: 'GZIP' },
  setup_rear_anti_roll_bar: { type: 'UINT_8', compression: 'GZIP' },
  setup_front_suspension_height: { type: 'UINT_8', compression: 'GZIP' },
  setup_rear_suspension_height: { type: 'UINT_8', compression: 'GZIP' },
  setup_brake_pressure: { type: 'UINT_8', compression: 'GZIP' },
  setup_brake_bias: { type: 'UINT_8', compression: 'GZIP' },
  setup_tyre_pressure_FL: { type: 'FLOAT', compression: 'GZIP' },
  setup_tyre_pressure_FR: { type: 'FLOAT', compression: 'GZIP' },
  setup_tyre_pressure_RL: { type: 'FLOAT', compression: 'GZIP' },
  setup_tyre_pressure_RR: { type: 'FLOAT', compression: 'GZIP' },
  setup_ballast: { type: 'UINT_8', compression: 'GZIP' },
  setup_fuel_load: { type: 'FLOAT', compression: 'GZIP' },
  speed: { type: 'UINT_16', compression: 'GZIP' },
  throttle: { type: 'FLOAT', compression: 'GZIP' },
  steer: { type: 'FLOAT', compression: 'GZIP' },
  brake: { type: 'FLOAT', compression: 'GZIP' },
  clutch: { type: 'UINT_8', compression: 'GZIP' },
  gear: { type: 'INT_8', compression: 'GZIP' },
  engine_rpm: { type: 'UINT_16', compression: 'GZIP' },
  drs: { type: 'UINT_8', compression: 'GZIP' },
  brake_temperature_FL: { type: 'UINT_16', compression: 'GZIP' },
  brake_temperature_FR: { type: 'UINT_16', compression: 'GZIP' },
  brake_temperature_RL: { type: 'UINT_16', compression: 'GZIP' },
  brake_temperature_RR: { type: 'UINT_16', compression: 'GZIP' },
  tyre_surface_temperature_FL: { type: 'UINT_8', compression: 'GZIP' },
  tyre_surface_temperature_FR: { type: 'UINT_8', compression: 'GZIP' },
  tyre_surface_temperature_RL: { type: 'UINT_8', compression: 'GZIP' },
  tyre_surface_temperature_RR: { type: 'UINT_8', compression: 'GZIP' },
  tyre_inner_temperature_FL: { type: 'UINT_8', compression: 'GZIP' },
  tyre_inner_temperature_FR: { type: 'UINT_8', compression: 'GZIP' },
  tyre_inner_temperature_RL: { type: 'UINT_8', compression: 'GZIP' },
  tyre_inner_temperature_RR: { type: 'UINT_8', compression: 'GZIP' },
  engine_temperature: { type: 'UINT_16', compression: 'GZIP' },
  tyre_pressure_FL: { type: 'FLOAT', compression: 'GZIP' },
  tyre_pressure_FR: { type: 'FLOAT', compression: 'GZIP' },
  tyre_pressure_RL: { type: 'FLOAT', compression: 'GZIP' },
  tyre_pressure_RR: { type: 'FLOAT', compression: 'GZIP' },
  traction_control: { type: 'UINT_8', compression: 'GZIP' },
  anti_lock_brakes: { type: 'UINT_8', compression: 'GZIP' },
  fuel_mix: { type: 'UINT_8', compression: 'GZIP' },
  front_brake_bias: { type: 'UINT_8', compression: 'GZIP' },
  pit_limiter_status: { type: 'UINT_8', compression: 'GZIP' },
  fuel_in_tank: { type: 'FLOAT', compression: 'GZIP' },
  drs_allowed: { type: 'UINT_8', compression: 'GZIP' },
  tyre_compound: { type: 'UINT_8', compression: 'GZIP' },
  tyre_visual_compound: { type: 'UINT_8', compression: 'GZIP' },
  tyre_age: { type: 'UINT_8', compression: 'GZIP' },
  marshalling_flags: { type: 'INT_8', compression: 'GZIP' },
  ers_store_energy: { type: 'FLOAT', compression: 'GZIP' },
  ers_deploy_mode: { type: 'UINT_8', compression: 'GZIP' },
  ers_harvested_lap_MGUK: { type: 'FLOAT', compression: 'GZIP' },
  ers_harvested_lap_MGUH: { type: 'FLOAT', compression: 'GZIP' },
  ers_deployed_lap: { type: 'FLOAT', compression: 'GZIP' },
  tyre_wear_FL: { type: 'FLOAT', compression: 'GZIP' },
  tyre_wear_FR: { type: 'FLOAT', compression: 'GZIP' },
  tyre_wear_RL: { type: 'FLOAT', compression: 'GZIP' },
  tyre_wear_RR: { type: 'FLOAT', compression: 'GZIP' },
  tyre_damage_FL: { type: 'UINT_8', compression: 'GZIP' },
  tyre_damage_FR: { type: 'UINT_8', compression: 'GZIP' },
  tyre_damage_RL: { type: 'UINT_8', compression: 'GZIP' },
  tyre_damage_RR: { type: 'UINT_8', compression: 'GZIP' },
  brake_damage_FL: { type: 'UINT_8', compression: 'GZIP' },
  brake_damage_FR: { type: 'UINT_8', compression: 'GZIP' },
  brake_damage_RL: { type: 'UINT_8', compression: 'GZIP' },
  brake_damage_RR: { type: 'UINT_8', compression: 'GZIP' },
  front_wing_damage_left: { type: 'UINT_8', compression: 'GZIP' },
  front_wing_damage_right: { type: 'UINT_8', compression: 'GZIP' },
  rear_wing_damage: { type: 'UINT_8', compression: 'GZIP' },
  floor_damage: { type: 'UINT_8', compression: 'GZIP' },
  diffuser_damage: { type: 'UINT_8', compression: 'GZIP' },
  sidepod_damage: { type: 'UINT_8', compression: 'GZIP' },
  drs_fault: { type: 'UINT_8', compression: 'GZIP' },
  gearbox_damage: { type: 'UINT_8', compression: 'GZIP' },
  engine_damage: { type: 'UINT_8', compression: 'GZIP' },
  engine_MGUH_wear: { type: 'UINT_8', compression: 'GZIP' },
  engine_ES_wear: { type: 'UINT_8', compression: 'GZIP' },
  engine_CE_wear: { type: 'UINT_8', compression: 'GZIP' },
  engine_ICE_wear: { type: 'UINT_8', compression: 'GZIP' },
  engine_MGUK_wear: { type: 'UINT_8', compression: 'GZIP' },
  engine_TC_wear: { type: 'UINT_8', compression: 'GZIP' },
});

const createMessage = (
  motionData,
  sessionData,
  lapData,
  participantData,
  setupData,
  telemetryData,
  statusData,
  damageData
) => {
  return {
    world_x: motionData.m_worldPositionX,
    world_y: motionData.m_worldPositionY,
    world_z: motionData.m_worldPositionZ,
    g_lateral: motionData.m_gForceLateral,
    g_longitudinal: motionData.m_gForceLongitudinal,
    g_vertical: motionData.m_gForceVertical,
    yaw: motionData.m_yaw,
    pitch: motionData.m_pitch,
    roll: motionData.m_roll,
    track_name: TRACKS[sessionData.m_trackId].name,
    track_length: sessionData.m_trackLength,
    session_type: SESSION_TYPES[sessionData.m_sessionType].long,
    total_laps: sessionData.m_weather,
    formula: FORMULAS[sessionData.m_formula],
    weather: WEATHER[sessionData.m_weather],
    track_temperature: sessionData.m_trackTemperature,
    air_temperature: sessionData.m_airTemperature,
    session_time_remaining: sessionData.m_sessionTimeLeft,
    safety_car_status: sessionData.m_safetyCarStatus,
    ai_difficulty: sessionData.m_aiDifficulty,
    pitstop_rejoin_position: sessionData.m_pitStopRejoinPosition,
    steering_assist: sessionData.m_steeringAssist,
    braking_assist: sessionData.m_brakingAssist,
    gearbox_assist: sessionData.m_gearboxAssist,
    pit_assist: sessionData.m_pitAssist,
    pit_release_assist: sessionData.m_pitReleaseAssist,
    ers_assist: sessionData.m_ERSAssist,
    drs_assist: sessionData.m_DRSAssist,
    last_lap_time: lapData.m_lastLapTimeInMS,
    lap_time: lapData.m_currentLapTimeInMS,
    sector_1_time: lapData.m_sector1TimeInMS,
    sector_2_time: lapData.m_sector2TimeInMS,
    lap_distance: lapData.m_lapDistance,
    total_distance: lapData.m_totalDistance,
    safety_car_delta: lapData.m_safetyCarDelta,
    car_position: lapData.m_carPosition,
    lap_number: lapData.m_currentLapNum,
    pit_status: lapData.m_pitStatus,
    sector: lapData.m_sector,
    lap_invalid: lapData.m_currentLapInvalid,
    penalties: lapData.m_penalties,
    warnings: lapData.m_warnings,
    grid_position: lapData.m_gridPosition,
    driver_status: lapData.m_driverStatus,
    result_status: lapData.m_resultStatus,
    participant_isAI: participantData.m_aiControlled,
    participant_network_ID: participantData.m_networkId,
    participant_first_name: DRIVERS[participantData.m_driverId].firstName,
    participant_last_name: DRIVERS[participantData.m_driverId].lastName,
    participant_abbreviation: DRIVERS[participantData.m_driverId].abbreviation,
    participant_nationality: NATIONALITIES[participantData.m_nationality]
      ? NATIONALITIES[participantData.m_nationality]
      : 'None',
    participant_name: participantData.m_name,
    participant_team_name: TEAMS[participantData.m_teamId].name,
    participant_race_number: participantData.m_raceNumber,
    participant_udp_visibility: participantData.m_yourTelemetry,
    setup_front_wing: setupData.m_frontWing,
    setup_rear_wing: setupData.m_rearWing,
    setup_diff_on_throttle: setupData.m_onThrottle,
    setup_diff_off_throttle: setupData.m_offThrottle,
    setup_front_camber: setupData.m_frontCamber,
    setup_rear_camber: setupData.m_rearCamber,
    setup_front_toe: setupData.m_frontToe,
    setup_rear_toe: setupData.m_rearToe,
    setup_front_suspension: setupData.m_frontSuspension,
    setup_rear_suspension: setupData.m_rearSuspension,
    setup_front_anti_roll_bar: setupData.m_frontAntiRollBar,
    setup_rear_anti_roll_bar: setupData.m_rearAntiRollBar,
    setup_front_suspension_height: setupData.m_frontSuspensionHeight,
    setup_rear_suspension_height: setupData.m_rearSuspensionHeight,
    setup_brake_pressure: setupData.m_brakePressure,
    setup_brake_bias: setupData.m_brakeBias,
    setup_tyre_pressure_FL: setupData.m_frontLeftTyrePressure,
    setup_tyre_pressure_FR: setupData.m_frontRightTyrePressure,
    setup_tyre_pressure_RL: setupData.m_rearLeftTyrePressure,
    setup_tyre_pressure_RR: setupData.m_rearRightTyrePressure,
    setup_ballast: setupData.m_ballast,
    setup_fuel_load: setupData.m_fuelLoad,
    speed: telemetryData.m_speed,
    throttle: telemetryData.m_throttle,
    steer: telemetryData.m_steer,
    brake: telemetryData.m_brake,
    clutch: telemetryData.m_clutch,
    gear: telemetryData.m_gear,
    engine_rpm: telemetryData.m_engineRPM,
    drs: telemetryData.m_drs,
    brake_temperature_FL: telemetryData.m_brakesTemperature[2],
    brake_temperature_FR: telemetryData.m_brakesTemperature[3],
    brake_temperature_RL: telemetryData.m_brakesTemperature[0],
    brake_temperature_RR: telemetryData.m_brakesTemperature[1],
    tyre_surface_temperature_FL: telemetryData.m_tyresSurfaceTemperature[2],
    tyre_surface_temperature_FR: telemetryData.m_tyresSurfaceTemperature[3],
    tyre_surface_temperature_RL: telemetryData.m_tyresSurfaceTemperature[0],
    tyre_surface_temperature_RR: telemetryData.m_tyresSurfaceTemperature[1],
    tyre_inner_temperature_FL: telemetryData.m_tyresInnerTemperature[2],
    tyre_inner_temperature_FR: telemetryData.m_tyresInnerTemperature[3],
    tyre_inner_temperature_RL: telemetryData.m_tyresInnerTemperature[0],
    tyre_inner_temperature_RR: telemetryData.m_tyresInnerTemperature[1],
    engine_temperature: telemetryData.m_engineTemperature,
    tyre_pressure_FL: telemetryData.m_tyresPressure[2],
    tyre_pressure_FR: telemetryData.m_tyresPressure[3],
    tyre_pressure_RL: telemetryData.m_tyresPressure[0],
    tyre_pressure_RR: telemetryData.m_tyresPressure[1],
    traction_control: statusData.m_tractionControl,
    anti_lock_brakes: statusData.m_antiLockBrakes,
    fuel_mix: statusData.m_fuelMix,
    front_brake_bias: statusData.m_frontBrakeBias,
    pit_limiter_status: statusData.m_pitLimiterStatus,
    fuel_in_tank: statusData.m_fuelInTank,
    drs_allowed: statusData.m_drsAllowed,
    tyre_compound: statusData.m_actualTyreCompound,
    tyre_visual_compound: statusData.m_visualTyreCompound,
    tyre_age: statusData.m_tyresAgeLaps,
    marshalling_flags: statusData.m_vehicleFiaFlags,
    ers_store_energy: statusData.m_ersStoreEnergy,
    ers_deploy_mode: statusData.m_ersDeployMode,
    ers_harvested_lap_MGUK: statusData.m_ersHarvestedThisLapMGUK,
    ers_harvested_lap_MGUH: statusData.m_ersHarvestedThisLapMGUH,
    ers_deployed_lap: statusData.m_ersDeployedThisLap,
    tyre_wear_FL: damageData.m_tyresWear[2],
    tyre_wear_FR: damageData.m_tyresWear[3],
    tyre_wear_RL: damageData.m_tyresWear[0],
    tyre_wear_RR: damageData.m_tyresWear[1],
    tyre_damage_FL: damageData.m_tyresDamage[2],
    tyre_damage_FR: damageData.m_tyresDamage[3],
    tyre_damage_RL: damageData.m_tyresDamage[0],
    tyre_damage_RR: damageData.m_tyresDamage[1],
    brake_damage_FL: damageData.m_brakesDamage[2],
    brake_damage_FR: damageData.m_brakesDamage[3],
    brake_damage_RL: damageData.m_brakesDamage[0],
    brake_damage_RR: damageData.m_brakesDamage[1],
    front_wing_damage_left: damageData.m_frontLeftWingDamage,
    front_wing_damage_right: damageData.m_frontRightWingDamage,
    rear_wing_damage: damageData.m_rearWingDamage,
    floor_damage: damageData.m_floorDamage,
    diffuser_damage: damageData.m_diffuserDamage,
    sidepod_damage: damageData.m_sidepodDamage,
    drs_fault: damageData.m_drsFault,
    gearbox_damage: damageData.m_gearBoxDamage,
    engine_damage: damageData.m_engineDamage,
    engine_MGUH_wear: damageData.m_engineMGUHWear,
    engine_ES_wear: damageData.m_engineESWear,
    engine_CE_wear: damageData.m_engineCEWear,
    engine_ICE_wear: damageData.m_engineICEWear,
    engine_MGUK_wear: damageData.m_engineMGUKWear,
    engine_TC_wear: damageData.m_engineTCWear,
  };
};

const schema = new ParquetSchema(createSchema());
const basePath = path.join(os.homedir(), `RaceHub`);

BigInt.prototype.toJSON = function () {
  return this.toString();
};

const readUInt16LE = (buffer, offset) => {
  return buffer[offset] | (buffer[offset + 1] << 8);
};

const readUInt8 = (buffer, offset) => {
  return buffer[offset];
};

const getFileStats = (filepath) => {
  const stats = fs.statSync(filepath)
  return {
    fileCreated: stats.ctime,
    fileModified: stats.mtime,
  }
}

const readFile = (filepath) => {
  console.log('Parsing ', filepath);
  try {
    const rawData = fs.readFileSync(filepath);
    const inflatedBuffer = Buffer.from(rawData);
    return zlib.unzipSync(inflatedBuffer);
  } catch (e) {
    console.log('Error parsing ', filepath);
    return [];
  }
};

const writeFileMetaInformation = (filepath, sessions) => {

  const sessionsListPath = `${basePath}/sessionsList.json`;

  const getSessionsList = () => {
    if (!fs.existsSync(sessionsListPath)) {
      return {};
    } else {
      const rawSessionsList = fs.readFileSync(sessionsListPath);
      return JSON.parse(rawSessionsList);
    }
  }

  const sessionsList = getSessionsList();

  sessions.forEach((session) => {
    const {
      uniqueSessionId,
      sessionId,
      filename,
      filepath,
      fileCreated,
      fileModified,
      sessionInfo,
      rowCount,
      participants,
      laps,
    } = session;
    const {
      trackName,
      trackLength,
      sessionType,
      totalLaps,
      trackTemperature,
      airTemperature,
      weather,
    } = sessionInfo;

    if (rowCount === 0) {
      return;
    }

    const metaFormattedParticipants = participants.reduce(
      (obj, p) => ({
        ...obj,
        [p.participantId]: {
          uniqueSessionId: p.uniqueSessionId,
          sessionId: p.sessionId,
          participantId: p.participantId,
          lapIds: p.lapIds,
          ...p.participantInfo,
          uploaded: false,
          filepath: `${basePath}/${path.parse(filename).name}/${uniqueSessionId}-${p.participantId}.parquet`,
        },
      }),
      {}
    );

    sessionMetadata = {
      uniqueSessionId,
      sessionId,
      filename,
      filepath,
      fileCreated,
      fileModified,
      trackName,
      trackLength,
      sessionType,
      totalLaps,
      trackTemperature,
      airTemperature,
      weather,
      rowCount,
      participants: metaFormattedParticipants,
      laps: laps.reduce((obj, cur) => ({...obj, [cur.lapId]: cur}), {}), //convert the array of laps to an o(id)
    };

    const sessionMetadataPath = `${basePath}/session-${uniqueSessionId}.json`;
    fs.writeFileSync(sessionMetadataPath, JSON.stringify(sessionMetadata));
  });

  sessionsList[filepath] = sessions.map((session) => session.uniqueSessionId);
  fs.writeFileSync(sessionsListPath, JSON.stringify(sessionsList));
};

const createWriter = async (filename, uniqueSessionId, participantId) => {
  const filenameWithoutExt = path.parse(filename).name;
  const outputDir = `${basePath}/${filenameWithoutExt}`;
  fs.mkdirSync(outputDir, {
    recursive: true,
  });
  return ParquetWriter.openFile(
    schema,
    `${outputDir}/${uniqueSessionId}-${participantId}.parquet`
  );
};

const writeParticipatMetaInformation = (
  writer,
  uniqueSessionId,
  sessionId,
  sessionInfo,
  participantInfo,
  laps,
  sessionHistory
) => {
  writer.setMetadata('sessionId', JSON.stringify(sessionId.toString()));
  writer.setMetadata('uniqueSessionId', JSON.stringify(uniqueSessionId));
  writer.setMetadata('session', JSON.stringify(sessionInfo));
  writer.setMetadata('participant', JSON.stringify(participantInfo));
  writer.setMetadata('laps', JSON.stringify(laps));
  writer.setMetadata('sessionHistory', JSON.stringify(sessionHistory));
};

const getParticipantInfo = (dataRow) => ({
  isPlayer: dataRow.playerCarIndex === dataRow.carIndex,
  isAi: dataRow.participant_isAI,
  firstName: dataRow.participant_first_name,
  lastName: dataRow.participant_last_name,
  abbreviation: dataRow.participant_abbreviation,
  nationality: dataRow.participant_nationality,
  name: dataRow.participant_name,
  teamName: dataRow.participant_team_name,
  raceNumber: dataRow.participant_race_number,
});

const getSessionInfo = (dataRow) => ({
  trackName: dataRow.track_name,
  trackLength: dataRow.track_length,
  sessionType: dataRow.session_type,
  totalLaps: dataRow.total_laps,
  formula: dataRow.forumula,
  trackTemperature: dataRow.track_temperature,
  airTemperature: dataRow.air_temperature,
  weather: dataRow.weather,
});

const createCurrentLap = (
  uniqueSessionId,
  packetSessionUId,
  participantId,
  sessionTime
) => ({
  lapId: nanoid(),
  uniqueSessionId,
  sessionId: packetSessionUId,
  participantId,
  startTime: sessionTime,
  endTime: null,
  isValid: true,
  isComplete: false,
  isInLap: false,
  isOutLap: false,
});

const createCurrentLapsArray = (
  uniqueSessionId,
  packetSessionUId,
  participantIds,
  sessionTime
) => {
  return Array(20)
    .fill(null)
    .map((i, index) =>
      createCurrentLap(
        uniqueSessionId,
        packetSessionUId,
        participantIds[index],
        sessionTime
      )
    );
};

const participantPerFile = async () => {
  const closeCurrentSession = async (sessionTime) => {
    if (sessions.length > 0) {
      const previousSession = sessions[sessions.length - 1];
      const previousRow = previousSession.latestRows[0];

      // did the previous session have any data points
      if (previousRow) {
        // add current laps
        if (currentLaps.length > 0) {
          currentLaps.forEach((lap, index) => {
            lap.isValid = !previousSession.latestRows[index].lap_invalid;
            lap.endTime = sessionTime;
            lap.isInLap = previousRow.driver_status === 2;
            lap.isOutLap = previousRow.driver_status === 3;
            previousSession.laps.push(lap);
            previousSession.participants[index].lapIds.push(lap.lapId);
          });
          // add laps to session
          previousSession.laps.push(...currentLaps);
        }

        // update session info
        const sessionInfo = getSessionInfo(previousRow);
        previousSession.sessionInfo = sessionInfo;

        // update participants
        previousSession.participants.forEach((participant, index) => {
          const participantInfo = getParticipantInfo(
            previousSession.latestRows[index]
          );
          participant.participantInfo = participantInfo;
        });

        // write metadata
        previousSession.parquetHandles.forEach((writer, index) => {
          const participantId =
            previousSession.participants[index].participantId;
          const participantInfo =
            previousSession.participants[index].participantInfo;
          const laps = previousSession.laps.filter(
            (lap) => lap.participantId === participantId
          );
          const sessionHistory = previousSession.sessionHistory[index];
          writeParticipatMetaInformation(
            writer,
            previousRow.uniqueSessionId,
            previousRow.sessionId,
            sessionInfo,
            participantInfo,
            laps,
            sessionHistory
          );
        });
      }

      const closeWriterPromises = previousSession.parquetHandles.map((writer) =>
        writer.close()
      );
      await Promise.all(closeWriterPromises);
    }
  };

  const { filename } = workerData;
  const filepath = `${basePath}/${filename}`;

  const buffer = readFile(filepath);

  const fileStats = getFileStats(filepath);
  const { fileCreated, fileModified } = fileStats;

  let offset = 8; //skip the header for now TODO: use the header

  let lastpacketSessionUId = null;
  let lastPacketFrameIdentifier = null;
  let sessionTime = null;
  let currentLaps = [];

  const sessions = [];

  while (offset < buffer.length) {
    console.log(`${((offset / buffer.length) * 100).toFixed(1)}`);

    const packetYear = readUInt16LE(buffer, offset);
    const packetType = readUInt8(buffer, offset + 5);
    const packetSize = getPacketSize(packetYear, packetType);

    const parsedMessage = parseBufferMessage(
      buffer.slice(offset, offset + packetSize),
      true
    );

    const { packetID, packetData } = parsedMessage;

    const packetSessionUId = packetData?.data.m_header.m_sessionUID;
    sessionTime = packetData?.data.m_header.m_sessionTime;
    const frameIdentifier = packetData?.data.m_header.m_frameIdentifier;
    const newParticipantIds = Array(20)
      .fill(null)
      .map(() => nanoid());

    if (packetSessionUId == 0) {
      lastpacketSessionUId = packetSessionUId;
      // next packet
      offset += packetSize;
      continue;
    }

    if (lastpacketSessionUId !== packetSessionUId) {
      // do we need to close a previous session?
      await closeCurrentSession(sessionTime);

      const uniqueSessionId = nanoid();
      
      // create writers
      const createWriterPromises = Array(20)
        .fill(null)
        .map((_, index) => {
          return createWriter(
            filename,
            uniqueSessionId,
            newParticipantIds[index]
          );
        });

      const parquetHandles = await Promise.all(createWriterPromises);

      const participants = Array(20)
        .fill(null)
        .map((i, index) => ({
          uniqueSessionId,
          sessionId: packetSessionUId,
          participantId: newParticipantIds[index],
          lapIds: [],
          participantInfo: {},
        }));

      const laps = [];
      currentLaps = createCurrentLapsArray(
        uniqueSessionId,
        packetSessionUId,
        newParticipantIds,
        sessionTime
      );

      const session = {
        filename,
        filepath,
        fileCreated,
        fileModified,
        uniqueSessionId,
        sessionId: packetSessionUId,
        parquetHandles,
        participants,
        laps,
        sessionHistory: Array(20)
          .fill(null)
          .map((i) => ({
            lapHistoryData: [],
            tyreHistoryData: [],
          })),
        sessionInfo: {},
        previousPacket: null,
        latestPackets: {
          [PACKETS.motion]: null,
          [PACKETS.session]: null,
          [PACKETS.lapData]: null,
          [PACKETS.participants]: null,
          [PACKETS.carSetups]: null,
          [PACKETS.carTelemetry]: null,
          [PACKETS.carStatus]: null,
          [PACKETS.carDamage]: null,
          [PACKETS.sessionHistory]: null,
        },
        previousRows: Array(20).fill(null),
        latestRows: Array(20).fill(null),
        rowCount: 0,
      };

      sessions.push(session);
    }

    const currentSession =
      sessions.length > 0 ? sessions[sessions.length - 1] : null;

    if (!currentSession) {
      offset += packetSize;
      continue;
    }

    if (frameIdentifier !== lastPacketFrameIdentifier) {
      const latestPackets = currentSession.latestPackets;
      if (!Object.values(latestPackets).some((v) => v === null)) {
        const writeMessagePromises = currentSession.parquetHandles.map(
          (w, index) => {
            const motionData =
              latestPackets[PACKETS.motion].packetData?.data.m_carMotionData[
                index
              ];
            const sessionData = latestPackets[PACKETS.session].packetData?.data;
            const lapData =
              latestPackets[PACKETS.lapData].packetData?.data.m_lapData[index];
            const participantData =
              latestPackets[PACKETS.participants].packetData?.data
                .m_participants[index];
            const setupData =
              latestPackets[PACKETS.carSetups].packetData?.data.m_carSetups[
                index
              ];
            const telemetryData =
              latestPackets[PACKETS.carTelemetry].packetData?.data
                .m_carTelemetryData[index];
            const statusData =
              latestPackets[PACKETS.carStatus].packetData?.data.m_carStatusData[
                index
              ];
            const damageData =
              latestPackets[PACKETS.carDamage].packetData?.data.m_carDamageData[
                index
              ];
            const isPlayerIdx =
              index === packetData?.data.m_header.m_playerCarIndex;
            const message = createMessage(
              motionData,
              sessionData,
              lapData,
              participantData,
              setupData,
              telemetryData,
              statusData,
              damageData
            );

            const row = {
              frameId: frameIdentifier ? frameIdentifier : 0,
              sessionTime: sessionTime,
              sessionId: `${packetSessionUId}`,
              uniqueSessionId: currentSession.uniqueSessionId,
              playerCarIndex: packetData?.data.m_header.m_playerCarIndex,
              carIndex: index,
              wheel_speed_FL: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSpeed[2]
                : 0,
              wheel_speed_FR: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSpeed[3]
                : 0,
              wheel_speed_RL: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSpeed[0]
                : 0,
              wheel_speed_RR: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSpeed[1]
                : 0,
              wheel_slip_FL: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSlip[2]
                : 0,
              wheel_slip_FR: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSlip[3]
                : 0,
              wheel_slip_RL: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSlip[0]
                : 0,
              wheel_slip_RR: isPlayerIdx
                ? latestPackets[PACKETS.motion].packetData?.data.m_wheelSlip[1]
                : 0,
              racing_line: isPlayerIdx ? sessionData.m_dynamicRacingLine : 0,
              racing_line_type: isPlayerIdx
                ? sessionData.m_dynamicRacingLineType
                : 0,
              ...message,
            };

            currentSession.previousRows[index] = currentSession.latestRows[index];
            currentSession.latestRows[index] = row;

            return w.appendRow(row);
          }
        );
        await Promise.all(writeMessagePromises);

        // update session info
        currentSession.rowCount++;

        // check laps
        currentLaps = currentLaps.map((currentLap, index) => {
          const currentRow = currentSession.latestRows[index];
          const previousRow = currentSession.previousRows[index];

          if (!previousRow) {
            return currentLap;
          }

          if (
            (currentRow.lap_time < previousRow.lap_time) ||
            (currentRow.lap_time > 0 && previousRow.lap_time == 0)
          ) {
            // add lap
            currentLap.isValid = !previousRow.lap_invalid;
            currentLap.endTime = previousRow.sessionTime;
            currentLap.isComplete = true;
            currentLap.isInLap = previousRow.driver_status === 2;
            currentLap.isOutLap = previousRow.driver_status === 3;
            currentSession.laps.push(currentLap);

            // create new lap
            const participantId =
              currentSession.participants[index].participantId;
            const newLap = createCurrentLap(
              currentSession.uniqueSessionId,
              packetSessionUId,
              participantId,
              sessionTime
            );
            currentSession.participants[index].lapIds.push(newLap.lapId);
            return newLap;
          } else {
            return currentLap;
          }
        });

        // add session history
        if (packetID === PACKETS.sessionHistory) {
          const carIndex = packetData?.data.m_carIdx;
          const lapHistoryData = packetData?.data.m_lapHistoryData.slice(
            0,
            packetData?.data.m_numLaps
          );
          const tyreHistoryData =
            packetData?.data.m_tyreStintsHistoryData.slice(
              0,
              packetData?.data.m_numTyreStints
            );
          currentSession.sessionHistory[carIndex] = {
            lapHistoryData,
            tyreHistoryData,
          };
        }
      }
    }

    currentSession.latestPackets[packetID] = parsedMessage;
    currentSession.previousPacket = parsedMessage;

    lastpacketSessionUId = packetSessionUId;
    lastPacketFrameIdentifier = frameIdentifier;

    // next packet
    offset += packetSize;
  }

  await closeCurrentSession(sessionTime);

  writeFileMetaInformation(filepath, sessions);

  console.log('Finished parsing');

  parentPort.postMessage({
    type: 'end',
  });
};

participantPerFile();
