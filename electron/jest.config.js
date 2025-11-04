module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/*.test.js'],
  coverageDirectory: 'coverage',
  collectCoverageFrom: [
    '**/*.js',
    '!**/*.test.js',
    '!jest.config.js',
    '!coverage/**',
    '!node_modules/**',
    '!dist/**'
  ],
  coverageReporters: ['text', 'lcov', 'html'],
  testTimeout: 10000,
  modulePathIgnorePatterns: ['<rootDir>/dist/']
};

