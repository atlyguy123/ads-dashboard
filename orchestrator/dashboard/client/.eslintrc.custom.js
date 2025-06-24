module.exports = {
  rules: {
    // Custom rule to prevent AVAILABLE_COLUMNS being defined outside of config/columns.js
    'no-available-columns-redefinition': {
      create(context) {
        return {
          VariableDeclarator(node) {
            if (node.id.name === 'AVAILABLE_COLUMNS') {
              const filename = context.getFilename();
              if (!filename.includes('config/columns.js')) {
                context.report({
                  node,
                  message: '❌ AVAILABLE_COLUMNS must only be defined in config/columns.js. Import it instead: import { AVAILABLE_COLUMNS } from "../config/columns"'
                });
              }
            }
          }
        };
      }
    }
  }
}; 