package usecase

// Dependencies point inward:
// controller -> usecase <- service -> repository ports <- infrastructure.
// Interfaces are owned here and are implemented by adapters at composition time.
