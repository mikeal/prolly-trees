const nocache = {
  has: () => false,
  get: () => { throw new Error('Cannot ask for entries from nocache') },
  set: () => {}
}

export { nocache }
