# Base image
FROM node:18-alpine

# Set working directory inside container
WORKDIR /app

# Copy files
COPY . .

# Install dependencies
RUN npm install

# Expose port
EXPOSE 3000

# Start command
CMD ["node", "index.js"]

COPY package*.json ./

CMD ["node", "index.js"]
