import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { inChannelFor, outChannelFor, sparkToSp1 } from './registry.js';

const SPARK =
  'spark1pgssyele0qrcjdheeq2a0zmpwdwvj3r4f4stkuju0fp36g6grapv2w7l8am2cp';

describe('sparkToSp1', () => {
  it('swaps the spark HRP for sp and keeps the identity', () => {
    const identifier = sparkToSp1(SPARK);
    assert.ok(identifier);
    assert.ok(identifier.startsWith('sp1'));
    assert.notEqual(identifier, SPARK);
    assert.equal(sparkToSp1(identifier), null);
    assert.equal(sparkToSp1('not-an-address'), null);
    assert.equal(sparkToSp1(''), null);
  });

  it('names the incoming and outgoing channels with the sp1 identifier', () => {
    const identifier = sparkToSp1(SPARK);
    assert.equal(
      inChannelFor('MAINNET', SPARK),
      `/transaction/in/MAINNET/${identifier}`,
    );
    assert.equal(
      outChannelFor('REGTEST', SPARK),
      `/transaction/out/REGTEST/${identifier}`,
    );
    assert.equal(inChannelFor('MAINNET', 'spark1bad'), null);
    assert.equal(outChannelFor('MAINNET', 'spark1bad'), null);
  });
});
