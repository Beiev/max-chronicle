// @ts-check
/**
 * The far sky: what is behind every galaxy of the memory, at no distance at
 * all. Only the direction the reader looks in moves it, so turning turns the
 * stars and coming closer does not.
 *
 * Nothing here is a memory. It is the night the memory hangs in, drawn the way
 * the night is:
 *
 *   stars      many faint and few bright, as stars are counted; most white,
 *              the hot ones blue-white, the cool ones yellow, orange and red;
 *              the brightest have rays and twinkle
 *   the band   a far galaxy seen from inside, edge on: a great circle of light
 *              round the sky, where the stars crowd, wider and warmer toward
 *              its heart, with lanes of dark dust along its middle
 *   nebulae    a few very faint clouds of colour, each in its own part of the
 *              sky, so that one part of the night is not like another and a
 *              reader who has turned around knows which way they face
 *   galaxies   too far to be anyone's: a smudge each, longer than wide
 *
 * No DOM and no drawing: this module says where the lights are and what they
 * look like; the renderer draws them, and node:test reads them too.
 */

import { mulberry32 } from "./model.js";
import { planeOf } from "./shape.js";

/** @typedef {[number, number, number]} Vec3 */

/** @param {number[]} v @returns {Vec3} */
function unit(v) {
  const length = Math.hypot(v[0], v[1], v[2]) || 1;
  return [v[0] / length, v[1] / length, v[2] / length];
}

/** The pole of the band, and two directions in its plane: how far round the band a direction lies is measured from the first toward the second. */
export const BAND_POLE = unit([0.38, 0.84, -0.39]);
export const [BAND_ACROSS, BAND_ALONG] = planeOf(BAND_POLE);
/** How far round the band its heart lies, in radians: to the right of where the page first looks, low in the window. */
export const BAND_HEART = -1.72;
/** How far off its middle the band's stars lie, as the sine of the angle: at the heart, and away from it. */
const BAND_WIDE = /** @type {[number, number]} */ ([0.3, 0.17]);

const FIELD_STARS = 12000;
const BAND_STARS = 12000;
const FAR_GALAXIES = 46;
/** Floats to a light: its direction (3), its colour (3), how strong it is, how many pixels across, a number of its own, how it leans and how flat it lies (a star has no flatness: 0). */
export const FAR = 11;

/**
 * The colours of stars by how hot they are, and how many in a hundred are of each: most are white or near it.
 * @type {[Vec3, number][]}
 */
const STAR_LIGHT = [
  [[0.7, 0.8, 1.0], 16], [[0.93, 0.95, 1.0], 40], [[1.0, 0.96, 0.84], 26], [[1.0, 0.82, 0.62], 13], [[1.0, 0.66, 0.5], 5],
];

/**
 * The clouds of colour: where each is, how far it reaches (radians), its colour and how strong it shows. They are
 * placed about where the page first looks from, so the first sight of the sky has two of them in it.
 * @type {{at: Vec3, reach: number, tint: Vec3, strength: number}[]}
 */
const CLOUDS = [
  { at: unit([0.83, -0.02, -0.55]), reach: 0.34, tint: [1.0, 0.4, 0.5], strength: 0.13 },    // rose, where hydrogen glows
  { at: unit([-0.02, 0.2, -1.0]), reach: 0.44, tint: [0.34, 0.56, 1.0], strength: 0.1 },     // blue, starlight on dust
  { at: unit([-0.6, 0.3, 0.74]), reach: 0.3, tint: [0.6, 0.44, 1.0], strength: 0.11 },       // violet
  { at: unit([0.2, 0.9, 0.39]), reach: 0.26, tint: [1.0, 0.68, 0.4], strength: 0.08 },       // amber
  { at: unit([-0.85, -0.4, 0.33]), reach: 0.22, tint: [0.32, 0.84, 0.74], strength: 0.08 },  // sea green
];
/** The clouds with two directions across each, for the renderer to read its painted wisps by. */
export const NEBULAE = CLOUDS.map((cloud, k) => {
  const [aside, over] = planeOf(cloud.at);
  return { ...cloud, aside, over, seed: [0.13 + k * 0.29, 0.61 + k * 0.17] };
});

/**
 * Every light of the far sky that is a point: the stars, then the far galaxies.
 * @returns {{lights: Float32Array, stars: number, count: number}} FAR floats to a light
 */
export function farSky() {
  const random = mulberry32(20261111), count = FIELD_STARS + BAND_STARS + FAR_GALAXIES;
  const lights = new Float32Array(count * FAR);
  /** @returns {Vec3} a direction, any as likely as another */
  const anywhere = () => {
    const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up);
    return [Math.cos(around) * ring, up, Math.sin(around) * ring];
  };
  /** A direction in the band: anywhere round it, more often toward its heart; a little off its middle, mostly less. @returns {{at: Vec3, heart: number}} */
  const inBand = () => {
    const toHeart = random() < 0.4, turn = toHeart ? BAND_HEART + (random() + random() + random() - 1.5) * 1.1 : random() * Math.PI * 2;
    const from = Math.abs(((turn - BAND_HEART + Math.PI * 3) % (Math.PI * 2)) - Math.PI), heart = Math.exp(-from * from / 0.5);
    const wide = BAND_WIDE[1] + (BAND_WIDE[0] - BAND_WIDE[1]) * heart, off = (random() + random() + random() - 1.5) * wide;
    return { at: unit([0, 1, 2].map((k) => Math.cos(turn) * BAND_ACROSS[k] + Math.sin(turn) * BAND_ALONG[k] + BAND_POLE[k] * off)), heart };
  };
  const total = STAR_LIGHT.reduce((sum, [, share]) => sum + share, 0);
  for (let k = 0; k < FIELD_STARS + BAND_STARS; k++) {
    const banded = k >= FIELD_STARS, place = banded ? inBand() : { at: anywhere(), heart: 0 };
    // Which light: by the count of each; toward the heart of the band the old warm stars are more.
    let pick = random() * total * (1 - 0.3 * place.heart) + total * 0.3 * place.heart, tint = STAR_LIGHT[0][0];
    for (const [light, share] of STAR_LIGHT) { tint = light; pick -= share; if (pick < 0) break; }
    // How bright: many faint, few bright: one in a hundred is a star to notice. A star of the band is one of a
    // crowd, and fainter for it. None is as bright as a memory: the far sky stays behind.
    const bright = Math.pow(random(), banded ? 16 : 12);
    lights.set([place.at[0], place.at[1], place.at[2], tint[0], tint[1], tint[2],
                (0.1 + 0.62 * Math.pow(bright, 0.45)) * (banded ? 0.75 : 1), 1.15 + 3.4 * Math.pow(bright, 1.2), random(), 0, 0], k * FAR);
  }
  for (let k = FIELD_STARS + BAND_STARS; k < count; k++) {
    // Far galaxies keep off the band: its dust hides them, as ours hides those behind it.
    let at = anywhere();
    while (Math.abs(at[0] * BAND_POLE[0] + at[1] * BAND_POLE[1] + at[2] * BAND_POLE[2]) < 0.2) at = anywhere();
    const warm = random(), tint = [1.0, 0.86 + 0.1 * warm, 0.66 + 0.3 * warm];
    lights.set([at[0], at[1], at[2], tint[0], tint[1], tint[2], 0.16 + random() * 0.24, 8 + random() * random() * 20, random(),
                random() * Math.PI, 0.22 + random() * 0.6], k * FAR);
  }
  return { lights, stars: FIELD_STARS + BAND_STARS, count };
}
