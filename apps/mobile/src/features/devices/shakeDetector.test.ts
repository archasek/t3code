import { describe, expect, it } from "vite-plus/test";

import { createShakeDetector } from "./shakeDetector";

const resting = (timestamp: number) => ({ x: 0, y: 0, z: -1, timestamp });
const jolt = (timestamp: number) => ({ x: 2.2, y: 0.4, z: -1, timestamp });

describe("shake detector", () => {
  it("ignores gravity and a single bump", () => {
    const detect = createShakeDetector();
    expect(detect(resting(0))).toBe(false);
    expect(detect(jolt(100))).toBe(false);
    expect(detect(resting(200))).toBe(false);
    expect(detect(jolt(900))).toBe(false);
  });

  it("reports two jolts within the window once, then cools down", () => {
    const detect = createShakeDetector();
    expect(detect(jolt(0))).toBe(false);
    expect(detect(resting(150))).toBe(false);
    expect(detect(jolt(300))).toBe(true);
    expect(detect(resting(350))).toBe(false);
    expect(detect(jolt(400))).toBe(false);
    expect(detect(resting(500))).toBe(false);
    expect(detect(jolt(600))).toBe(false);
    expect(detect(resting(1_300))).toBe(false);
    expect(detect(jolt(1_400))).toBe(false);
    expect(detect(resting(1_450))).toBe(false);
    expect(detect(jolt(1_500))).toBe(true);
  });

  it("counts a sustained excursion sampled every 50ms as only one jolt", () => {
    const detect = createShakeDetector();
    for (let timestamp = 0; timestamp <= 600; timestamp += 50)
      expect(detect(jolt(timestamp))).toBe(false);
  });

  it("does not count a high excursion that started during cooldown after cooldown ends", () => {
    const detect = createShakeDetector();
    expect(detect(jolt(0))).toBe(false);
    expect(detect(resting(50))).toBe(false);
    expect(detect(jolt(100))).toBe(true);
    expect(detect(resting(150))).toBe(false);
    expect(detect(jolt(1_050))).toBe(false);
    expect(detect(jolt(1_100))).toBe(false);
    expect(detect(resting(1_150))).toBe(false);
    expect(detect(jolt(1_200))).toBe(false);
    expect(detect(resting(1_250))).toBe(false);
    expect(detect(jolt(1_300))).toBe(true);
  });
});
