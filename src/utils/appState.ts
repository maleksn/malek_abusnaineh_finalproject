let isReady = false;

export function markAppReady(): boolean {
  if (isReady) {
    return false;
  }

  isReady = true;
  return true;
}

export function markAppNotReady(): boolean {
  if (!isReady) {
    return false;
  }

  isReady = false;
  return true;
}

export function checkAppReady(): boolean {
  return isReady;
}