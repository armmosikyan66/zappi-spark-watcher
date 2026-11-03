export type WatchedKind = 'wallet' | 'pot';

export type WatchedAddress = {
  sparkAddress: string;
  kind: WatchedKind;
  /** 33-byte hex identity pubkey. Kept for the hint envelope; matching is by channel. */
  identityPubkey?: string;
};

export type SubscriptionDiff = {
  subscribe: WatchedAddress[];
  unsubscribe: string[];
};

export function diffSubscriptions(
  current: ReadonlyMap<string, WatchedAddress>,
  next: readonly WatchedAddress[],
): SubscriptionDiff {
  const desired = new Map<string, WatchedAddress>();
  for (const row of next) {
    if (!row.sparkAddress.trim()) continue;
    desired.set(row.sparkAddress, row);
  }
  const subscribe: WatchedAddress[] = [];
  for (const [sparkAddress, row] of desired) {
    if (!current.has(sparkAddress)) subscribe.push(row);
  }
  const unsubscribe: string[] = [];
  for (const [sparkAddress] of current) {
    // The desired set is the online-wallet wallets plus every pot someone
    // currently has open. Viewer rooms live in Redis with 45s leases, so a
    // missing pot really has no viewer — dropping it here also clears a pot
    // whose viewer left while Nest was down and never got to send its
    // explicit unsubscribe.
    if (!desired.has(sparkAddress)) unsubscribe.push(sparkAddress);
  }
  return { subscribe, unsubscribe };
}

const CHARSET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
const BECH32M_CONST = 0x2bc830a3;

/**
 * Sparkscan's per-address channels match the sp1 identifier, not the spark1
 * receive address. Same bech32m data; only the HRP and checksum differ.
 */
export function sparkToSp1(sparkAddress: string): string | null {
  const decoded = bech32Decode(sparkAddress.trim().toLowerCase());
  if (!decoded || decoded.hrp !== 'spark') return null;
  return bech32mEncode('sp', decoded.data);
}

/** Incoming transactions for one identity. Null when the address will not decode. */
export function inChannelFor(
  network: string,
  sparkAddress: string,
): string | null {
  const identifier = sparkToSp1(sparkAddress);
  if (!identifier) return null;
  return `/transaction/in/${network}/${identifier}`;
}

/** Outgoing transactions for one identity. */
export function outChannelFor(
  network: string,
  sparkAddress: string,
): string | null {
  const identifier = sparkToSp1(sparkAddress);
  if (!identifier) return null;
  return `/transaction/out/${network}/${identifier}`;
}

function bech32Polymod(values: readonly number[]): number {
  const gen = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
  let chk = 1;
  for (const value of values) {
    const top = chk >>> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ value;
    for (let bit = 0; bit < 5; bit += 1) {
      if ((top >>> bit) & 1) chk ^= gen[bit] ?? 0;
    }
  }
  return chk;
}

function bech32HrpExpand(hrp: string): number[] {
  const out: number[] = [];
  for (const char of hrp) out.push(char.charCodeAt(0) >>> 5);
  out.push(0);
  for (const char of hrp) out.push(char.charCodeAt(0) & 31);
  return out;
}

function bech32Decode(
  value: string,
): { hrp: string; data: number[] } | null {
  const separator = value.lastIndexOf('1');
  if (separator < 1 || separator + 7 > value.length || value.length > 90) {
    return null;
  }
  const hrp = value.slice(0, separator);
  const data: number[] = [];
  for (const char of value.slice(separator + 1)) {
    const index = CHARSET.indexOf(char);
    if (index === -1) return null;
    data.push(index);
  }
  if (bech32Polymod([...bech32HrpExpand(hrp), ...data]) !== BECH32M_CONST) {
    return null;
  }
  return { hrp, data: data.slice(0, -6) };
}

function bech32mEncode(hrp: string, data: readonly number[]): string {
  const values = [...bech32HrpExpand(hrp), ...data, 0, 0, 0, 0, 0, 0];
  const polymod = bech32Polymod(values) ^ BECH32M_CONST;
  const checksum: number[] = [];
  for (let index = 0; index < 6; index += 1) {
    checksum.push((polymod >>> (5 * (5 - index))) & 31);
  }
  return (
    hrp +
    '1' +
    [...data, ...checksum].map((part) => CHARSET[part] ?? '').join('')
  );
}
